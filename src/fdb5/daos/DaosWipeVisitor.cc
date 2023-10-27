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

#include "fdb5/daos/DaosWipeVisitor.h"
#include "fdb5/daos/DaosName.h"
#include "fdb5/daos/DaosSession.h"

#include <fdb5/LibFdb5.h>

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosWipeVisitor::DaosWipeVisitor(const DaosCatalogue& catalogue,
                                 const Store& store,
                                 const metkit::mars::MarsRequest& request,
                                 std::ostream& out,
                                 bool doit,
                                 bool porcelain,
                                 bool unsafeWipeAll) :
    WipeVisitor(request, out, doit, porcelain, unsafeWipeAll),
    catalogue_(catalogue),
    store_(store), 
    dbKvPath_("") {}

DaosWipeVisitor::~DaosWipeVisitor() {}

bool DaosWipeVisitor::visitDatabase(const Catalogue& catalogue, const Store& store) {

    // Overall checks

    ASSERT(&catalogue_ == &catalogue);
    ASSERT(catalogue.enabled(ControlIdentifier::Wipe));
    /// @todo: this seems to be restarting the wipe visiting?
    WipeVisitor::visitDatabase(catalogue, store);

    // Check that we are in a clean state (i.e. we only visit one DB).

    ASSERT(indexPaths_.empty());
    ASSERT(axisPaths_.empty());
    ASSERT(safeKvPaths_.empty());

    ASSERT(storePaths_.empty());
    ASSERT(safeStorePaths_.empty());

    ASSERT(!dbKvPath_.poolName().size());

    // Having selected a DB, construct the residual request. This is the request that is used for
    // matching Index(es) -- which is relevant if there is subselection of the DB.

    indexRequest_ = request_;
    for (const auto& kv : catalogue.key()) {
        indexRequest_.unsetValues(kv.first);
    }

    return true; // Explore contained indexes
}

bool DaosWipeVisitor::visitIndex(const Index& index) {

    /// @todo: why isn't WipeVisitor::visitIndex being called here?

    fdb5::DaosKeyValueName location{index.location().uri()};
    const fdb5::DaosKeyValueName& db_kv = catalogue_.dbKeyValue();

    // Is this index matched by the supplied request?
    // n.b. If the request is over-specified (i.e. below the index level), nothing will be removed

    bool include = index.key().match(indexRequest_);

    // If we have cross fdb-mounted another DB, ensure we can't delete another DBs data.
    if (!(location.contName() == db_kv.contName() && 
          location.poolName() == db_kv.poolName())) {
        include = false;
    }
    // ASSERT(location.dirName().sameAs(basePath) || !include);

    // Add the axis and index paths to be removed.

    std::set<fdb5::DaosKeyValueName> axes;
    fdb5::DaosSession s{};
    fdb5::DaosKeyValue index_kv{s, location};  /// @todo: asserts that kv exists
    daos_size_t size = index_kv.size("axes");
    if (size > 0) {
        std::vector<char> axes_data((long) size);
        index_kv.get("axes", &axes_data[0], size);
        std::vector<std::string> axis_names;
        eckit::Tokenizer parse(",");
        parse(std::string(axes_data.begin(), axes_data.end()), axis_names);
        std::string idx{index.key().valuesToString()};
        for (const auto& name : axis_names) {
            /// @todo: take oclass from config
            fdb5::DaosKeyValueOID oid(idx + std::string{"."} + name, OC_S1);
            fdb5::DaosKeyValueName nkv(location.poolName(), location.contName(), oid);
            nkv.generateOID();
            axes.insert(nkv);
        }
    }

    if (include) {
        indexPaths_.insert(location);
        axisPaths_.insert(axes.begin(), axes.end());
    } else {
        // This will ensure that if only some indexes are to be removed from a file, then
        // they will be masked out but the file not deleted.
        safeKvPaths_.insert(location);
        safeKvPaths_.insert(axes.begin(), axes.end());
    }

    // Enumerate data files.

    /// @note: although a daos index will point to only one daos store container if using a daos store, 
    ///   a daos index can point to multiple posix store files (one per IO server process) if using a posix store.
    std::vector<eckit::URI> indexDataPaths(index.dataPaths());
    for (const eckit::URI& uri : store_.asStoreUnitURIs(indexDataPaths)) {
        if (include) {
            if (!store_.uriBelongs(uri)) {
                Log::error() << "Index to be deleted has pointers to fields that don't belong to the configured store." << std::endl;
                Log::error() << "Impossible to delete such fields. Index deletion aborted to avoid leaking fields." << std::endl;
                NOTIMP;
            }
            storePaths_.insert(uri);
        } else {
            safeStorePaths_.insert(uri);
        }
    }

    return true; // Explore contained entries

}

void DaosWipeVisitor::ensureSafePaths() {

    // Very explicitly ensure that we cannot delete anything marked as safe

    // if (dbKvPath_.poolName().size()) {
    //     fdb5::DaosKeyValueName db_kv{dbKvPath_.poolName(), dbKvPath_.contName(), dbKvPath_.OID()};
    //     if (safeKvPaths_.find(db_kv) != safeKvPaths_.end()) dbKvPath_ = fdb5::DaosName("");
    // }

    // for (const auto& p : safeKvPaths_) {
    //     indexPaths_.erase(p);
    //     axisPaths_.erase(p);
    // }

    for (const auto& p : safeStorePaths_)
        storePaths_.erase(p);

}

void DaosWipeVisitor::calculateResidualPaths() {

    // Remove paths to non-existant files. This is reasonable as we may be recovering from a
    // previous failed, partial wipe. As such, referenced files may not exist any more.

    /// @note: indexPaths_ always exist at this point, as their existence is ensured in DaosCatalogue::indexes
    /// @note: axisPaths_ existence cannot be verified as kvs "always exist" in DAOS

    // Consider the total sets of paths in the DB

    std::set<fdb5::DaosKeyValueName> deleteKvPaths;
    deleteKvPaths.insert(indexPaths_.begin(), indexPaths_.end());
    deleteKvPaths.insert(axisPaths_.begin(), axisPaths_.end());
    if (dbKvPath_.poolName().size()) deleteKvPaths.insert(dbKvPath_.URI());

    std::set<fdb5::DaosKeyValueName> allKvPaths;
    /// @note: given a database container, list DB kv, index kvs and axis kvs
    const fdb5::DaosKeyValueName& db_kv = catalogue_.dbKeyValue();
    fdb5::DaosName db_cont{db_kv.poolName(), db_kv.contName()};
    for (const auto& oid : db_cont.listOIDs()) {
        if (oid.otype() == DAOS_OT_KV_HASHED) {
            allKvPaths.insert(fdb5::DaosKeyValueName{db_cont.poolName(), db_cont.contName(), oid});
        } else if (oid.otype() != DAOS_OT_ARRAY_BYTE) {
            throw SeriousBug("Found non-KV non-ByteArray objects in DB container " + db_cont.URI().asString());
        }
    }

    ASSERT(residualKvPaths_.empty());

    if (!(deleteKvPaths == allKvPaths)) {

        // First we check if there are paths marked to delete that don't exist. This is an error

        std::set<fdb5::DaosKeyValueName> paths;
        std::set_difference(deleteKvPaths.begin(), deleteKvPaths.end(),
                            allKvPaths.begin(), allKvPaths.end(),
                            std::inserter(paths, paths.begin()));

        if (!paths.empty()) {
            Log::error() << "DaosKeyValueNames not in existing names set:" << std::endl;
            for (const auto& p : paths) {
                Log::error() << " - " << p.URI() << std::endl;
            }
            throw SeriousBug("KV names to delete in deleteKvPaths should should be in existing name set. Are multiple wipe commands running simultaneously?", Here());
        }

        std::set_difference(allKvPaths.begin(), allKvPaths.end(),
                            deleteKvPaths.begin(), deleteKvPaths.end(),
                            std::inserter(residualKvPaths_, residualKvPaths_.begin()));
    }

    // repeat the algorithm for store units (store files or containers)

    for (std::set<eckit::URI>* fileset : {&storePaths_}) {
        for (std::set<eckit::URI>::iterator it = fileset->begin(); it != fileset->end(); ) {

            if (store_.uriExists(*it)) {
                ++it;
            } else {
                fileset->erase(it++);
            }

        }
    }

    std::set<eckit::URI> deleteStorePaths;
    deleteStorePaths.insert(storePaths_.begin(), storePaths_.end());

    std::vector<eckit::URI> allStorePathsVector;
    for (const auto& u : store_.storeUnitURIs()) {
        allStorePathsVector.push_back(u);
    }
    std::set<eckit::URI> allStorePaths(allStorePathsVector.begin(), allStorePathsVector.end());

    ASSERT(residualStorePaths_.empty());

    if (!(storePaths_ == allStorePaths)) {

        // First we check if there are paths marked to delete that don't exist. This is an error

        std::set<eckit::URI> paths;
        std::set_difference(storePaths_.begin(), storePaths_.end(),
                            allStorePaths.begin(), allStorePaths.end(),
                            std::inserter(paths, paths.begin()));

        if (!paths.empty()) {
            Log::error() << "Store units (store paths or containers) not in existing paths set:" << std::endl;
            for (const auto& p : paths) {
                Log::error() << " - " << p << std::endl;
            }
            throw SeriousBug("Store unit to delete should be in existing path set. Are multiple wipe commands running simultaneously?", Here());
        }

        std::set_difference(allStorePaths.begin(), allStorePaths.end(),
                            storePaths_.begin(), storePaths_.end(),
                            std::inserter(residualStorePaths_, residualStorePaths_.begin()));

    }

}

bool DaosWipeVisitor::anythingToWipe() const {
    return (!indexPaths_.empty() || !axisPaths_.empty() || !storePaths_.empty() || dbKvPath_.poolName().size());
}

void DaosWipeVisitor::report(bool wipeAll) {

    ASSERT(anythingToWipe());

    if (wipeAll) {

        out_ << "DB container to delete:" << std::endl;
        const fdb5::DaosKeyValueName& db_kv = catalogue_.dbKeyValue(); 
        out_ << "    " << fdb5::DaosName{db_kv.poolName(), db_kv.contName()}.URI() << std::endl;
        out_ << std::endl;

        out_ << "DB KV to delete:" << std::endl;
        out_ << "    " << db_kv.URI() << std::endl;
        out_ << std::endl;

        if (store_.type() != "daos") {
            out_ << "Store URI to delete:" << std::endl;
            out_ << "    " << store_.uri() << std::endl;
            out_ << std::endl;
        }

    } else {

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
    if (indexPaths_.empty()) out_ << " - NONE -" << std::endl;
    for (const auto& f : indexPaths_) {
        out_ << "    " << f.URI() << std::endl;
    }
    out_ << std::endl;

    out_ << "Axis KVs to delete: " << std::endl;
    if (axisPaths_.empty()) out_ << " - NONE -" << std::endl;
    for (const auto& f : axisPaths_) {
        out_ << "    " << f.URI() << std::endl;
    }
    out_ << std::endl;

    out_ << "Store units (store files or arrays) to delete: " << std::endl;
    if (storePaths_.empty()) out_ << " - NONE -" << std::endl;
    for (const auto& f : storePaths_) {
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
        for (const eckit::URI& uri : residualStorePaths_) {
            if (store_.uriExists(uri)) {
                store_.remove(uri, logAlways, logVerbose, doit_);
            }
        }
    }

    if (!wipeAll || (wipeAll && store_.type() != "daos")) {
        for (const eckit::URI& uri : storePaths_) {
            store_.remove(uri, logAlways, logVerbose, doit_);
        }
    }
    if (!wipeAll) {
        for (const fdb5::DaosKeyValueName& name : axisPaths_) {
            if (name.exists()) {
                catalogue_.remove(name, logAlways, logVerbose, doit_);
            }
        }
        for (const fdb5::DaosKeyValueName& name : indexPaths_) {
            if (name.exists()) {

                fdb5::DaosSession s{};
                fdb5::DaosKeyValue index_kv{s, name};
                daos_size_t size = index_kv.size("key");
                std::vector<char> idxkey_data((long) size);
                index_kv.get("key", &idxkey_data[0], size);
                eckit::MemoryStream ms{&idxkey_data[0], size};
                fdb5::Key key(ms);
                std::string idx{key.valuesToString()};

                catalogue_.remove(name, logAlways, logVerbose, doit_);

                fdb5::DaosKeyValue db_kv{s, catalogue_.dbKeyValue()};
                db_kv.remove(idx);

            } else {
                NOTIMP;
                /// iterate catalogue kv to clean dangling reference
                /// expensive
            }
        }
    }

    if (wipeAll) {

        if (store_.type() != "daos")
            /// @todo: if the store is holding catalogue information (e.g. index files) it 
            ///    should not be removed
            store_.remove(store_.uri(), logAlways, logVerbose, doit_);

        const fdb5::DaosKeyValueName& db_kv = catalogue_.dbKeyValue();
        const fdb5::DaosKeyValueName& root_kv = catalogue_.rootKeyValue();

        fdb5::DaosName db_cont{db_kv.poolName(), db_kv.contName()};

        if (db_cont.exists()) db_cont.destroy();
        
        std::string db_key = db_kv.contName();
        if (root_kv.has(db_key)) {
            fdb5::DaosSession s{};
            fdb5::DaosKeyValue root{s, root_kv};
            root.remove(db_key);
        }
    }

}


void DaosWipeVisitor::catalogueComplete(const Catalogue& catalogue) {

    /// @todo: this is restarting catalogueComplete? or calling EntryVisitor's catalogueComplete?
    WipeVisitor::catalogueComplete(catalogue);

    // We wipe everything if there is nothingn within safe paths - i.e. there is
    // no data that wasn't matched by the request (either because all DB indexes were matched
    // or because the entire DB was matched)

    bool wipeAll = safeKvPaths_.empty() && safeStorePaths_.empty();

    dbKvPath_ = fdb5::DaosName("");
    if (wipeAll) {
        const fdb5::DaosKeyValueName& n = catalogue_.dbKeyValue();
        dbKvPath_ = fdb5::DaosName(n.poolName(), n.contName(), n.OID());
    }

    ensureSafePaths();

    if (anythingToWipe()) {

        if (wipeAll) calculateResidualPaths();

        if (!porcelain_) report(wipeAll);

        // This is here as it needs to run whatever combination of doit/porcelain/...
        if (wipeAll && !residualKvPaths_.empty()) {

            out_ << "Unexpected KVs present in DB container: " << std::endl;
            for (const auto& p : residualKvPaths_) out_ << "    " << p.URI() << std::endl;
            out_ << std::endl;

        }
        if (wipeAll && !residualStorePaths_.empty()) {

            out_ << "Unexpected store units (store files or arrays) present in store: " << std::endl;
            for (const auto& p : residualStorePaths_) out_ << "    " << store_.type() << "://" << p << std::endl;
            out_ << std::endl;

        }
        if (wipeAll && (!residualKvPaths_.empty() || !residualStorePaths_.empty())) {
            if (!unsafeWipeAll_) {
                out_ << "Full wipe will not proceed without --unsafe-wipe-all" << std::endl;
                if (doit_)
                    throw Exception("Cannot fully wipe unclean Daos DB", Here());
            }
        }

        if (doit_ || porcelain_) wipe(wipeAll);
    }
}


//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
