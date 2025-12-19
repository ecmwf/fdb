/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Log.h"
#include "eckit/serialisation/MemoryStream.h"

#include "fdb5/daos/DaosName.h"
#include "fdb5/daos/DaosSession.h"
#include "fdb5/daos/DaosWipeVisitor.h"
#include "fdb5/database/Store.h"

#include <fdb5/LibFdb5.h>

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosWipeVisitor::DaosWipeVisitor(const DaosCatalogue& catalogue, const Store& store,
                                 const metkit::mars::MarsRequest& request, std::ostream& out, bool doit, bool porcelain,
                                 bool unsafeWipeAll) :
    WipeVisitor(request, out, doit, porcelain, unsafeWipeAll), catalogue_(catalogue), store_(store), dbKvName_("") {}

DaosWipeVisitor::~DaosWipeVisitor() {}

bool DaosWipeVisitor::visitDatabase(const Catalogue& catalogue) {

    // Overall checks

    ASSERT(&catalogue_ == &catalogue);
    ASSERT(catalogue.enabled(ControlIdentifier::Wipe));
    WipeVisitor::visitDatabase(catalogue);

    // Check that we are in a clean state (i.e. we only visit one DB).

    ASSERT(indexNames_.empty());
    ASSERT(axisNames_.empty());
    ASSERT(safeKvNames_.empty());

    ASSERT(storeURIs_.empty());
    ASSERT(safeStoreURIs_.empty());

    ASSERT(!dbKvName_.poolName().size());

    // Having selected a DB, construct the residual request. This is the request that is used for
    // matching Index(es) -- which is relevant if there is subselection of the DB.

    indexRequest_ = request_;
    for (const auto& kv : catalogue.key()) {
        indexRequest_.unsetValues(kv.first);
    }

    return true;  // Explore contained indexes
}

bool DaosWipeVisitor::visitIndex(const Index& index) {

    fdb5::DaosKeyValueName location{index.location().uri()};
    const fdb5::DaosKeyValueName& db_kv = catalogue_.dbKeyValue();

    // Is this index matched by the supplied request?
    // n.b. If the request is over-specified (i.e. below the index level), nothing will be removed

    bool include = index.key().match(indexRequest_);

    // If we have cross fdb-mounted another DB, ensure we can't delete another DBs data.
    if (!(location.containerName() == db_kv.containerName() && location.poolName() == db_kv.poolName())) {
        include = false;
    }
    // ASSERT(location.dirName().sameAs(basePath) || !include);

    // Add the axis and index paths to be removed.

    std::set<fdb5::DaosKeyValueName> axes;
    fdb5::DaosSession s{};
    fdb5::DaosKeyValue index_kv{s, location};  /// @todo: asserts that kv exists
    uint64_t size = index_kv.size("axes");
    if (size > 0) {
        std::vector<char> axes_data(size);
        index_kv.get("axes", &axes_data[0], size);
        std::vector<std::string> axis_names;
        eckit::Tokenizer parse(",");
        parse(std::string(axes_data.begin(), axes_data.end()), axis_names);
        std::string idx{index.key().valuesToString()};
        for (const auto& name : axis_names) {
            /// @todo: take oclass from config
            fdb5::DaosKeyValueOID oid(idx + std::string{"."} + name, OC_S1);
            fdb5::DaosKeyValueName nkv(location.poolName(), location.containerName(), oid);
            axes.insert(nkv);
        }
    }

    if (include) {
        indexNames_.insert(location);
        axisNames_.insert(axes.begin(), axes.end());
    }
    else {
        safeKvNames_.insert(location);
        safeKvNames_.insert(axes.begin(), axes.end());
    }

    // Enumerate data files.

    /// @note: although a daos index will point to only one daos store container if using a daos store,
    ///   a daos index can point to multiple posix store files (one per IO server process) if using a posix store.
    std::vector<eckit::URI> indexDataURIs(index.dataURIs());
    for (const eckit::URI& uri : store_.asCollocatedDataURIs(indexDataURIs)) {
        if (include) {
            if (!store_.uriBelongs(uri)) {
                Log::error() << "Index to be deleted has pointers to fields that don't belong to the configured store."
                             << std::endl;
                Log::error() << "Configured Store URI: " << store_.uri().asString() << std::endl;
                Log::error() << "Pointed Store unit URI: " << uri.asString() << std::endl;
                Log::error() << "Impossible to delete such fields. Index deletion aborted to avoid leaking fields."
                             << std::endl;
                throw eckit::SeriousBug{"Index deletion aborted to avoid leaking fields."};
            }
            storeURIs_.insert(uri);
        }
        else {
            safeStoreURIs_.insert(uri);
        }
    }

    return true;  // Explore contained entries
}

void DaosWipeVisitor::ensureSafeURIs() {

    // Very explicitly ensure that we cannot delete anything marked as safe

    for (const auto& p : safeStoreURIs_) {
        storeURIs_.erase(p);
    }
}

void DaosWipeVisitor::calculateResidualURIs() {

    // Remove URIs to non-existant objects. This is reasonable as we may be recovering from a
    // previous failed, partial wipe. As such, referenced objects may not exist any more.

    /// @note: indexNames_ always exist at this point, as their existence is ensured in DaosCatalogue::indexes
    /// @note: axisNames_ existence cannot be verified as kvs "always exist" in DAOS

    // Consider the total sets of URIs in the DB

    std::set<fdb5::DaosKeyValueName> deleteKvNames;
    deleteKvNames.insert(indexNames_.begin(), indexNames_.end());
    deleteKvNames.insert(axisNames_.begin(), axisNames_.end());
    if (dbKvName_.poolName().size()) {
        deleteKvNames.insert(dbKvName_.URI());
    }

    std::set<fdb5::DaosKeyValueName> allKvNames;
    /// @note: given a database container, list DB kv, index kvs and axis kvs
    const fdb5::DaosKeyValueName& db_kv = catalogue_.dbKeyValue();
    fdb5::DaosName db_cont{db_kv.poolName(), db_kv.containerName()};
    for (const auto& oid : db_cont.listOIDs()) {
        if (oid.otype() == DAOS_OT_KV_HASHED) {
            allKvNames.insert(fdb5::DaosKeyValueName{db_cont.poolName(), db_cont.containerName(), oid});
        }
        else if (oid.otype() != DAOS_OT_ARRAY_BYTE) {
            throw SeriousBug("Found non-KV non-ByteArray objects in DB container " + db_cont.URI().asString());
        }
    }

    ASSERT(residualKvNames_.empty());

    if (!(deleteKvNames == allKvNames)) {

        // First we check if there are names marked to delete that don't exist. This is an error

        std::set<fdb5::DaosKeyValueName> names;
        std::set_difference(deleteKvNames.begin(), deleteKvNames.end(), allKvNames.begin(), allKvNames.end(),
                            std::inserter(names, names.begin()));

        if (!names.empty()) {
            Log::error() << "DaosKeyValueNames not in existing names set:" << std::endl;
            for (const auto& n : names) {
                Log::error() << " - " << n.URI() << std::endl;
            }
            throw SeriousBug(
                "KV names to delete in deleteKvNames should should be in existing name set. Are multiple wipe commands "
                "running simultaneously?",
                Here());
        }

        std::set_difference(allKvNames.begin(), allKvNames.end(), deleteKvNames.begin(), deleteKvNames.end(),
                            std::inserter(residualKvNames_, residualKvNames_.begin()));
    }

    // repeat the algorithm for store units (store files or containers)

    for (std::set<eckit::URI>* uriset : {&storeURIs_}) {
        for (std::set<eckit::URI>::iterator it = uriset->begin(); it != uriset->end();) {

            if (store_.uriExists(*it)) {
                ++it;
            }
            else {
                uriset->erase(it++);
            }
        }
    }

    std::set<eckit::URI> deleteStoreURIs;
    deleteStoreURIs.insert(storeURIs_.begin(), storeURIs_.end());

    std::vector<eckit::URI> allStoreURIsVector;
    for (const auto& u : store_.collocatedDataURIs()) {
        allStoreURIsVector.push_back(u);
    }
    std::set<eckit::URI> allStoreURIs(allStoreURIsVector.begin(), allStoreURIsVector.end());

    ASSERT(residualStoreURIs_.empty());

    if (!(storeURIs_ == allStoreURIs)) {

        // First we check if there are URIs marked to delete that don't exist. This is an error

        std::set<eckit::URI> uris;
        std::set_difference(storeURIs_.begin(), storeURIs_.end(), allStoreURIs.begin(), allStoreURIs.end(),
                            std::inserter(uris, uris.begin()));

        if (!uris.empty()) {
            Log::error() << "Store units (store paths or containers) not in existing paths set:" << std::endl;
            for (const auto& u : uris) {
                Log::error() << " - " << u << std::endl;
            }
            throw SeriousBug(
                "Store unit to delete should be in existing URI set. Are multiple wipe commands running "
                "simultaneously?",
                Here());
        }

        std::set_difference(allStoreURIs.begin(), allStoreURIs.end(), storeURIs_.begin(), storeURIs_.end(),
                            std::inserter(residualStoreURIs_, residualStoreURIs_.begin()));
    }
}

bool DaosWipeVisitor::anythingToWipe() const {
    return (!indexNames_.empty() || !axisNames_.empty() || !storeURIs_.empty() || dbKvName_.poolName().size());
}

void DaosWipeVisitor::report(bool wipeAll) {

    ASSERT(anythingToWipe());

    if (wipeAll) {

        out_ << "DB container to delete:" << std::endl;
        const fdb5::DaosKeyValueName& db_kv = catalogue_.dbKeyValue();
        out_ << "    " << fdb5::DaosName{db_kv.poolName(), db_kv.containerName()}.URI() << std::endl;
        out_ << std::endl;

        out_ << "DB KV to delete:" << std::endl;
        out_ << "    " << db_kv.URI() << std::endl;
        out_ << std::endl;

        if (store_.type() != "daos") {
            out_ << "Store URI to delete:" << std::endl;
            out_ << "    " << store_.uri() << std::endl;
            out_ << std::endl;
        }
    }
    else {

        out_ << "DB container to delete:" << std::endl;
        out_ << " - NONE -" << std::endl;
        out_ << std::endl;
        out_ << "DB KV to delete:" << std::endl;
        out_ << " - NONE -" << std::endl;
        out_ << std::endl;
        if (store_.type() != "daos") {
            out_ << "Store URI to delete:" << std::endl;
            out_ << " - NONE -" << std::endl;
            out_ << std::endl;
        }
    }

    out_ << "Index KVs to delete: " << std::endl;
    if (indexNames_.empty()) {
        out_ << " - NONE -" << std::endl;
    }
    for (const auto& n : indexNames_) {
        out_ << "    " << n.URI() << std::endl;
    }
    out_ << std::endl;

    out_ << "Axis KVs to delete: " << std::endl;
    if (axisNames_.empty()) {
        out_ << " - NONE -" << std::endl;
    }
    for (const auto& n : axisNames_) {
        out_ << "    " << n.URI() << std::endl;
    }
    out_ << std::endl;

    out_ << "Store units (store files or arrays) to delete: " << std::endl;
    if (storeURIs_.empty()) {
        out_ << " - NONE -" << std::endl;
    }
    for (const auto& f : storeURIs_) {
        out_ << "    " << f << std::endl;
    }
    out_ << std::endl;
}

void DaosWipeVisitor::wipe(bool wipeAll) {

    ASSERT(anythingToWipe());

    std::ostream& logAlways(out_);
    std::ostream& logVerbose(porcelain_ ? Log::debug<LibFdb5>() : out_);

    // Now we want to do the actual deletion
    // n.b. We delete carefully in a order such that we can always access the DB by what is left

    /// @note: only need to remove residual store paths if the store is not daos. If store is daos,
    ///   then the entire container will be removed together with residual paths and
    ///   residual kv paths if wipeAll is true
    if (store_.type() != "daos") {
        for (const eckit::URI& uri : residualStoreURIs_) {
            if (store_.uriExists(uri)) {
                store_.remove(uri, logAlways, logVerbose, doit_);
            }
        }
    }

    if (!wipeAll || (wipeAll && store_.type() != "daos")) {
        for (const eckit::URI& uri : storeURIs_) {
            store_.remove(uri, logAlways, logVerbose, doit_);
        }
    }
    if (!wipeAll) {
        for (const fdb5::DaosKeyValueName& name : axisNames_) {
            if (name.exists()) {
                catalogue_.remove(name, logAlways, logVerbose, doit_);
            }
        }
        for (const fdb5::DaosKeyValueName& name : indexNames_) {
            if (name.exists()) {

                fdb5::DaosSession s{};
                fdb5::DaosKeyValue index_kv{s, name};
                std::vector<char> data;
                eckit::MemoryStream ms = index_kv.getMemoryStream(data, "key", "index kv");
                fdb5::Key key(ms);
                std::string idx{key.valuesToString()};

                catalogue_.remove(name, logAlways, logVerbose, doit_);

                fdb5::DaosKeyValue db_kv{s, catalogue_.dbKeyValue()};
                if (doit_) {
                    db_kv.remove(idx);
                }
            }
            else {
                NOTIMP;
                /// iterate catalogue kv to clean dangling reference
                /// expensive
            }
        }
    }

    if (wipeAll) {

        if (store_.type() != "daos") {
            /// @todo: if the store is holding catalogue information (e.g. index files) it
            ///    should not be removed
            store_.remove(store_.uri(), logAlways, logVerbose, doit_);
        }

        const fdb5::DaosKeyValueName& db_kv = catalogue_.dbKeyValue();
        const fdb5::DaosKeyValueName& root_kv = catalogue_.rootKeyValue();

        fdb5::DaosName db_cont{db_kv.poolName(), db_kv.containerName()};

        if (db_cont.exists() && doit_) {
            db_cont.destroy();
        }

        std::string db_key = db_kv.containerName();
        fdb5::DaosSession s{};
        fdb5::DaosKeyValue root{s, root_kv};
        if (root.has(db_key) && doit_) {
            root.remove(db_key);
        }
    }
}


void DaosWipeVisitor::catalogueComplete(const Catalogue& catalogue) {

    WipeVisitor::catalogueComplete(catalogue);

    // We wipe everything if there is nothingn within safe paths - i.e. there is
    // no data that wasn't matched by the request (either because all DB indexes were matched
    // or because the entire DB was matched)

    bool wipeAll = safeKvNames_.empty() && safeStoreURIs_.empty();

    dbKvName_ = fdb5::DaosName("");
    if (wipeAll) {
        const fdb5::DaosKeyValueName& n = catalogue_.dbKeyValue();
        dbKvName_ = fdb5::DaosName(n.poolName(), n.containerName(), n.OID());
    }

    ensureSafeURIs();

    if (anythingToWipe()) {

        if (wipeAll) {
            calculateResidualURIs();
        }

        if (!porcelain_) {
            report(wipeAll);
        }

        // This is here as it needs to run whatever combination of doit/porcelain/...
        if (wipeAll && !residualKvNames_.empty()) {

            out_ << "Unexpected KVs present in DB container: " << std::endl;
            for (const auto& n : residualKvNames_) {
                out_ << "    " << n.URI() << std::endl;
            }
            out_ << std::endl;
        }
        if (wipeAll && !residualStoreURIs_.empty()) {

            out_ << "Unexpected store units (store files or arrays) present in store: " << std::endl;
            for (const auto& u : residualStoreURIs_) {
                out_ << "    " << store_.type() << "://" << u << std::endl;
            }
            out_ << std::endl;
        }
        if (wipeAll && (!residualKvNames_.empty() || !residualStoreURIs_.empty())) {
            if (!unsafeWipeAll_) {
                out_ << "Full wipe will not proceed without --unsafe-wipe-all" << std::endl;
                if (doit_) {
                    throw Exception("Cannot fully wipe unclean Daos DB", Here());
                }
            }
        }

        if (doit_ || porcelain_) {
            wipe(wipeAll);
        }
    }
}


//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
