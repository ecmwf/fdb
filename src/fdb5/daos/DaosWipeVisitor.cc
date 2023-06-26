/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

// #include <algorithm>

// #include "eckit/os/Stat.h"
#include "eckit/log/Log.h"
#include "eckit/serialisation/MemoryStream.h"

// #include "fdb5/api/helpers/ControlIterator.h"
// #include "fdb5/database/DB.h"
// #include "fdb5/toc/TocCatalogue.h"
#include "fdb5/daos/DaosWipeVisitor.h"
#include "fdb5/daos/DaosName.h"
#include "fdb5/daos/DaosSession.h"

// #include <dirent.h>
// #include <errno.h>
#include <fdb5/LibFdb5.h>
// #include <sys/types.h>
// #include <cstring>

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

// namespace {
// class StdDir {

//     eckit::PathName path_;
//     DIR *d_;

// public:

//     StdDir(const eckit::PathName& p) :
//         path_(p),
//         d_(opendir(p.localPath())) {

//         if (!d_) {
//             std::stringstream ss;
//             ss << "Failed to open directory " << p << " (" << errno << "): " << strerror(errno);
//             throw eckit::SeriousBug(ss.str(), Here());
//         }
//     }

//     ~StdDir() { if (d_) closedir(d_); }

//     void children(std::vector<eckit::PathName>& paths) {

//         // Implemented here as PathName::match() does not return hidden files starting with '.'

//         struct dirent* e;

//         while ((e = readdir(d_)) != nullptr) {

//             if (e->d_name[0] == '.') {
//                 if (e->d_name[1] == '\0' || (e->d_name[1] == '.' && e->d_name[2] == '\0')) continue;
//             }

//             eckit::PathName p(path_ / e->d_name);

//             eckit::Stat::Struct info;
//             SYSCALL(eckit::Stat::lstat(p.localPath(), &info));

//             if (S_ISDIR(info.st_mode)) {
//                 StdDir d(p);
//                 d.children(paths);
//             }

//             // n.b. added after all children
//             paths.push_back(p);
//         }
//     }
// };
// }

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
    // schemaPath_("") {}

DaosWipeVisitor::~DaosWipeVisitor() {}

bool DaosWipeVisitor::visitDatabase(const Catalogue& catalogue, const Store& store) {

    // Overall checks

    ASSERT(&catalogue_ == &catalogue);
//    ASSERT(&store_ == &store);
    ASSERT(catalogue.enabled(ControlIdentifier::Wipe));
    /// @todo: this seems to be restarting the wipe visiting?
    WipeVisitor::visitDatabase(catalogue, store);

    // Check that we are in a clean state (i.e. we only visit one DB).

    // ASSERT(subtocPaths_.empty());
    // ASSERT(lockfilePaths_.empty());
    ASSERT(indexPaths_.empty());
    ASSERT(axisPaths_.empty());
    ASSERT(safeKvPaths_.empty());

    ASSERT(storePaths_.empty());
    ASSERT(safeStorePaths_.empty());
    // ASSERT(indexesToMask_.empty());

    // ASSERT(!tocPath_.asString().size());
    ASSERT(!dbKvPath_.poolName().size());
    // ASSERT(!schemaPath_.asString().size());

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

    // ruleFor

    // Key key(keyFingerprint, catalogue_->schema().ruleFor(catalogue_->key(), index.key()));
    std::set<fdb5::DaosKeyValueName> axes;
    if (location.exists() && location.has("axes")) {
        fdb5::DaosSession s{};
        fdb5::DaosKeyValue index_kv{s, location};
        daos_size_t size = index_kv.size("axes");
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
        // indexesToMask_.push_back(index);
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
    store_.asStoreUnitURIs(indexDataPaths);
    for (const eckit::URI& uri : indexDataPaths) {
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

// void TocWipeVisitor::addMaskedPaths() {

//     //ASSERT(indexRequest_.empty());

//     std::set<std::pair<eckit::URI, Offset>> metadata;
//     std::set<eckit::URI> data;
//     catalogue_.allMasked(metadata, data);
//     for (const auto& entry : metadata) {
//         eckit::PathName path = entry.first.path();
//         if (path.dirName().sameAs(catalogue_.basePath())) {
//             if (path.baseName().asString().substr(0, 4) == "toc.") {
//                 subtocPaths_.insert(path);
//             } else {
//                 indexPaths_.insert(path);
//             }
//         }
//     }
//     for (const auto& uri : data) {
//         if (store_.uriBelongs(uri)) dataPaths_.insert(store_.getStoreUnitPath(uri));
//     }
// }

// void DaosWipeVisitor::addMetadataPaths() {

    // // toc, schema

    // schemaPath_ = catalogue_.schemaPath();
    // tocPath_ = catalogue_.tocPath();

    // // subtocs

    // const auto&& subtocs(catalogue_.subTocPaths());
    // subtocPaths_.insert(subtocs.begin(), subtocs.end());

    // // lockfiles

    // const auto&& lockfiles(catalogue_.lockfilePaths());
    // lockfilePaths_.insert(lockfiles.begin(), lockfiles.end());
// }

void DaosWipeVisitor::ensureSafePaths() {

    // Very explicitly ensure that we cannot delete anything marked as safe

    if (dbKvPath_.poolName().size()) {
        fdb5::DaosKeyValueName db_kv{dbKvPath_.poolName(), dbKvPath_.contName(), dbKvPath_.OID()};
        if (safeKvPaths_.find(db_kv) != safeKvPaths_.end()) dbKvPath_ = fdb5::DaosName("");
    }
    // if (safePaths_.find(schemaPath_) != safePaths_.end()) schemaPath_ = "";

    for (const auto& p : safeKvPaths_) {
        // for (std::set<PathName>* s : {&subtocPaths_, &lockfilePaths_, &indexPaths_, &dataPaths_}) {
        indexPaths_.erase(p);
        axisPaths_.erase(p);
        // }
    }

    for (const auto& p : safeStorePaths_)
        storePaths_.erase(p);

}

void DaosWipeVisitor::calculateResidualPaths() {

    // Remove paths to non-existant files. This is reasonable as we may be recovering from a
    // previous failed, partial wipe. As such, referenced files may not exist any more.

    for (std::set<fdb5::DaosKeyValueName>* fileset : {&indexPaths_, &axisPaths_}) {
        for (std::set<fdb5::DaosKeyValueName>::iterator it = fileset->begin(); it != fileset->end(); ) {

            if (it->exists()) {
                ++it;
            } else {
                fileset->erase(it++);
            }

        }
    }

    if (dbKvPath_.poolName().size() && !dbKvPath_.exists()) dbKvPath_ = fdb5::DaosName("");

    // if (schemaPath_.asString().size() && !schemaPath_.exists())
    //     schemaPath_ = "";

    // Consider the total sets of paths in the DB (including store if its of same type as catalogue)

    std::set<fdb5::DaosKeyValueName> deleteKvPaths;
    // deletePaths.insert(subtocPaths_.begin(), subtocPaths_.end());
    // deletePaths.insert(lockfilePaths_.begin(), lockfilePaths_.end());
    deleteKvPaths.insert(indexPaths_.begin(), indexPaths_.end());
    deleteKvPaths.insert(axisPaths_.begin(), axisPaths_.end());
    // if (store_.type() == catalogue_.type())
    //     deletePaths.insert(dataPaths_.begin(), dataPaths_.end());
    if (dbKvPath_.poolName().size()) deleteKvPaths.insert(dbKvPath_.URI());
    // if (schemaPath_.asString().size())
    //     deletePaths.insert(schemaPath_);

    std::set<fdb5::DaosKeyValueName> allKvPaths;
    /// @note: given a database container, list DB kv, index kvs and axis kvs
    const fdb5::DaosKeyValueName& db_kv = catalogue_.dbKeyValue();
    fdb5::DaosName db_cont{db_kv.poolName(), db_kv.contName()};
    for (const auto& oid : db_cont.listOIDs()) {
        if (oid.otype() != DAOS_OT_KV_HASHED)
            throw SeriousBug("Found non-KV objects in DB container " + db_cont.URI().asString());
        allKvPaths.insert(fdb5::DaosKeyValueName{db_cont.poolName(), db_cont.contName(), oid});
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

    // if (store_.type() == "daos") return;

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

    // out_ << "FDB owner: " << catalogue_.owner() << std::endl
    //      << std::endl;

    if (wipeAll) {

        out_ << "DB container to delete:" << std::endl;
        const fdb5::DaosKeyValueName& db_kv = catalogue_.dbKeyValue(); 
        out_ << "    " << fdb5::DaosName{db_kv.poolName(), db_kv.contName()}.URI() << std::endl;
    //     for (const auto& f : subtocPaths_) {
    //         out_ << "    " << f << std::endl;
    //     }
        out_ << std::endl;

        out_ << "DB KV to delete:" << std::endl;
        out_ << "    " << db_kv.URI() << std::endl;
    //     for (const auto& f : subtocPaths_) {
    //         out_ << "    " << f << std::endl;
    //     }
        out_ << std::endl;

    } else {

        out_ << "DB container to delete:" << std::endl;
        out_ << " - NONE -" << std::endl;
        out_ << std::endl;
        out_ << "DB KV to delete:" << std::endl;
        out_ << " - NONE -" << std::endl;
        out_ << std::endl;

    }

//     out_ << "Control files to delete:" << std::endl;
//     if (!schemaPath_.asString().size() && lockfilePaths_.empty()) out_ << " - NONE -" << std::endl;
//     if (schemaPath_.asString().size()) out_ << "    " << schemaPath_ << std::endl;
//     for (const auto& f : lockfilePaths_) {
//         out_ << "    " << f << std::endl;
//     }
//     out_ << std::endl;

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

    out_ << "Store units (store files or containers) to delete: " << std::endl;
    if (storePaths_.empty()) out_ << " - NONE -" << std::endl;
    for (const auto& f : storePaths_) {
        out_ << "    " << f << std::endl;
    }
    out_ << std::endl;

//     out_ << "Protected files (explicitly untouched):" << std::endl;
//     if (safePaths_.empty()) out_ << " - NONE - " << std::endl;
//     for (const auto& f : safePaths_) {
//         out_ << "    " << f << std::endl;
//     }
//     out_ << std::endl;

//     if (!safePaths_.empty()) {
//         out_ << "Indexes to mask:" << std::endl;
//         if (indexesToMask_.empty()) out_ << " - NONE - " << std::endl;
//         for (const auto& i : indexesToMask_) {
//             out_ << "    " << i.location() << std::endl;
//         }
//     }

}

void DaosWipeVisitor::wipe(bool wipeAll) {

    ASSERT(anythingToWipe());

    std::ostream& logAlways(out_);
    std::ostream& logVerbose(porcelain_ ? Log::debug<LibFdb5>() : out_);

//     // Sanity checks...

//     catalogue_.checkUID();

//     // If we are wiping the metadata files, then we need to lock the DB to ensure we don't get
//     // into a state we don't like.

//     if (wipeAll && doit_) {
//         catalogue_.control(ControlAction::Disable, ControlIdentifier::List |
//                                          ControlIdentifier::Retrieve |
//                                          ControlIdentifier::Archive);

//         ASSERT(!catalogue_.enabled(ControlIdentifier::List));
//         ASSERT(!catalogue_.enabled(ControlIdentifier::Retrieve));
//         ASSERT(!catalogue_.enabled(ControlIdentifier::Archive));

//         // The lock will have occurred after the visitation phase, so add the lockfiles.
//         const auto&& lockfiles(catalogue_.lockfilePaths());
//         lockfilePaths_.insert(lockfiles.begin(), lockfiles.end());
//     }

//     // If we are wiping only a subset, and as a result have indexes to mask out; do that first.
//     // This results in a failure mode merely being data becoming invisible (which has the correct
//     // effect for the user), to be wiped at a later date.

//     if (!indexesToMask_.empty() && !wipeAll) {
//         for (const auto& index : indexesToMask_) {
//             logVerbose << "Index to mask: ";
//             logAlways << index << std::endl;
//             if (doit_) catalogue_.maskIndexEntry(index);
//         }
//     }

    // Now we want to do the actual deletion
    // n.b. We delete carefully in a order such that we can always access the DB by what is left
    for (const eckit::URI& uri : residualStorePaths_) {
        if (store_.uriExists(uri)) {
            store_.remove(uri, logAlways, logVerbose, doit_);
        }
    }
    if (!wipeAll) {
        for (const fdb5::DaosKeyValueName& name : residualKvPaths_) {
            if (name.exists()) {
                catalogue_.remove(name, logAlways, logVerbose, doit_);
            }
        }
    }

    for (const eckit::URI& uri : storePaths_) {
        store_.remove(uri, logAlways, logVerbose, doit_);
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

//     for (const std::set<PathName>& pathset : {indexPaths_,
//                                               std::set<PathName>{schemaPath_}, subtocPaths_,
//                                               std::set<PathName>{tocPath_}, lockfilePaths_,
//                                               (wipeAll ? std::set<PathName>{catalogue_.basePath()} : std::set<PathName>{})}) {

//         for (const PathName& path : pathset) {
//             if (path.exists()) {
//                 catalogue_.remove(path, logAlways, logVerbose, doit_);
//             }
//         }
//     }

}


void DaosWipeVisitor::catalogueComplete(const Catalogue& catalogue) {

    /// @todo: this is restarting catalogueComplete? or calling EntryVisitor's catalogueComplete?
    WipeVisitor::catalogueComplete(catalogue);

    // We wipe everything if there is nothingn within safePaths - i.e. there is
    // no data that wasn't matched by the request

    bool wipeAll = safeKvPaths_.empty() && safeStorePaths_.empty();

    dbKvPath_ = fdb5::DaosName("");
    if (wipeAll) {
        const fdb5::DaosKeyValueName& n = catalogue_.dbKeyValue();
        dbKvPath_ = fdb5::DaosName(n.poolName(), n.contName(), n.OID());
        // addMaskedPaths();
        // addMetadataPaths();
    }
    // } else {
        // Ensure we _really_ don't delete these if not wiping everything
        // subtocPaths_.clear();
        // lockfilePaths_.clear();
        // schemaPath_ = "";
    // }

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

            out_ << "Unexpected store units (store files or containers) present in store: " << std::endl;
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
