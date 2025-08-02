/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include <dirent.h>
#include <fdb5/LibFdb5.h>
#include <sys/types.h>
#include <algorithm>
#include <cerrno>
#include <cstring>

#include "eckit/log/Plural.h"
#include "eckit/os/Stat.h"

#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Store.h"
#include "fdb5/toc/TocCatalogue.h"
#include "fdb5/toc/TocWipeVisitor.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

namespace {
class StdDir {

    eckit::PathName path_;
    DIR* d_;

public:

    StdDir(const eckit::PathName& p) : path_(p), d_(opendir(p.localPath())) {

        if (!d_) {
            std::stringstream ss;
            ss << "Failed to open directory " << p << " (" << errno << "): " << strerror(errno);
            throw eckit::SeriousBug(ss.str(), Here());
        }
    }

    ~StdDir() {
        if (d_)
            closedir(d_);
    }

    void children(std::vector<eckit::PathName>& paths) {

        // Implemented here as PathName::match() does not return hidden files starting with '.'

        struct dirent* e;

        while ((e = readdir(d_)) != nullptr) {

            if (e->d_name[0] == '.') {
                if (e->d_name[1] == '\0' || (e->d_name[1] == '.' && e->d_name[2] == '\0'))
                    continue;
            }

            eckit::PathName p(path_ / e->d_name);

            eckit::Stat::Struct info;
            SYSCALL(eckit::Stat::lstat(p.localPath(), &info));

            if (S_ISDIR(info.st_mode)) {
                StdDir d(p);
                d.children(paths);
            }

            // n.b. added after all children
            paths.push_back(p);
        }
    }
};
}  // namespace

//----------------------------------------------------------------------------------------------------------------------

// TODO: Warnings and errors form inside here back to the user.

TocWipeVisitor::TocWipeVisitor(const TocCatalogue& catalogue,
                               const metkit::mars::MarsRequest& request, eckit::Queue<WipeElement>& queue,
                               bool doit, bool porcelain, bool unsafeWipeAll) :
    WipeVisitor(catalogue.config(), request, queue, /*out,*/ doit, porcelain, unsafeWipeAll),
    catalogue_(catalogue),
    tocPath_(""),
    schemaPath_("") {}

TocWipeVisitor::~TocWipeVisitor() {}

bool TocWipeVisitor::visitDatabase(const Catalogue& catalogue) {

    // Overall checks

    ASSERT(&catalogue_ == &catalogue);
    ASSERT(catalogue.enabled(ControlIdentifier::Wipe));
    WipeVisitor::visitDatabase(catalogue);

    // Check that we are in a clean state (i.e. we only visit one DB).
    ASSERT(subtocPaths_.empty());
    ASSERT(lockfilePaths_.empty());
    ASSERT(indexPaths_.empty());
    ASSERT(safeCataloguePaths_.empty());
    ASSERT(indexesToMask_.empty());

    ASSERT(!tocPath_.asString().size());
    ASSERT(!schemaPath_.asString().size());

    ASSERT(stores_.empty());

    // Having selected a DB, construct the residual request. This is the request that is used for
    // matching Index(es) -- which is relevant if there is subselection of the DB.

    indexRequest_ = request_;
    for (const auto& kv : catalogue.key()) {
        indexRequest_.unsetValues(kv.first);
    }

    return true;  // Explore contained indexes
}

bool TocWipeVisitor::visitIndex(const Index& index) {

    eckit::PathName location(index.location().uri().path());
    const auto& basePath(catalogue_.basePath());

    // Is this index matched by the supplied request?
    // n.b. If the request is over-specified (i.e. below the index level), nothing will be removed

    bool include = index.key().match(indexRequest_);

    // If we have cross fdb-mounted another DB, ensure we can't delete another DBs data.
    if (!location.dirName().sameAs(basePath)) {
        include = false;
    }
    ASSERT(location.dirName().sameAs(basePath) || !include);

    // Add the index paths to be removed.

    if (include) {
        indexesToMask_.push_back(index);
        indexPaths_.insert(location);
    }
    else {
        // This will ensure that if only some indexes are to be removed from a file, then
        // they will be masked out but the file not deleted.
        safeCataloguePaths_.insert(location);
    }

    // Enumerate data files
    storeWipeElements_.reset();
    
    std::map<eckit::URI, std::vector<eckit::URI>> dataURIbyStore;
    for (auto& dataURI : index.dataURIs()) {
        auto storeURI = StoreFactory::instance().uri(dataURI);
        auto storeIt = stores_.find(storeURI);
        if (storeIt == stores_.end()) {
            auto store = StoreFactory::instance().build(storeURI, config_);
            ASSERT(store);
            storeIt = stores_.emplace(storeURI, std::move(store)).first;
        }
        auto dataURIsIt = dataURIbyStore.find(storeURI);
        if (dataURIsIt == dataURIbyStore.end()) {
            dataURIsIt = dataURIbyStore.emplace(storeURI, std::vector<eckit::URI>{}).first; 
        }
        dataURIsIt->second.push_back(dataURI);
    }
    bool canWipe = true;
    for (const auto& [storeURI, dataURIs] : dataURIbyStore) {
        auto storeIt = stores_.find(storeURI);
        ASSERT(storeIt != stores_.end());

        canWipe = canWipe && storeIt->second->canWipe(dataURIs, unsafeWipeAll_);
    }
    return canWipe;
}

void TocWipeVisitor::addMaskedPaths() {

    // ASSERT(indexRequest_.empty());

    std::set<std::pair<eckit::URI, Offset>> metadata;
    std::set<eckit::URI> data;
    catalogue_.allMasked(metadata, data);
    for (const auto& entry : metadata) {
        eckit::PathName path = entry.first.path();
        if (path.dirName().sameAs(catalogue_.basePath())) {
            if (path.baseName().asString().substr(0, 4) == "toc.") {
                subtocPaths_.insert(path);
            }
            else {
                indexPaths_.insert(path);
            }
        }
    }
    /// @todo move to TocStore

    // for (const auto& uri : data) {
    //     if (store_.uriBelongs(uri)) {
    //         dataPaths_.insert(uri.path());
    //         auto auxPaths = getAuxiliaryPaths(uri);
    //         auxiliaryDataPaths_.insert(auxPaths.begin(), auxPaths.end());
    //     }
    // }
}

void TocWipeVisitor::addMetadataPaths() {

    // schema, toc

    schemaPath_ = catalogue_.schemaPath().path();
    tocPath_    = catalogue_.tocPath().path();

    // subtocs

    const auto&& subtocs(catalogue_.subTocPaths());
    subtocPaths_.insert(subtocs.begin(), subtocs.end());

    // lockfiles

    const auto&& lockfiles(catalogue_.lockfilePaths());
    lockfilePaths_.insert(lockfiles.begin(), lockfiles.end());
}

void TocWipeVisitor::ensureSafePaths() {

    // Very explicitly ensure that we cannot delete anything marked as safe

    if (safeCataloguePaths_.find(tocPath_) != safeCataloguePaths_.end())
        tocPath_ = "";
    if (safeCataloguePaths_.find(schemaPath_) != safeCataloguePaths_.end())
        schemaPath_ = "";

    for (const auto& p : safeCataloguePaths_) {
        for (std::set<PathName>* s :
             {&subtocPaths_, &lockfilePaths_, &indexPaths_}) {
            s->erase(p);
        }
    }
}
void TocWipeVisitor::calculateResidualPaths() {

    // Remove paths to non-existant files. This is reasonable as we may be recovering from a
    // previous failed, partial wipe. As such, referenced files may not exist any more.
    // NB: Not needed for auxiliaryDataPaths_ as their existence is checked in getAuxiliaryPaths()

    for (std::set<PathName>* fileset : {&subtocPaths_, &lockfilePaths_, &indexPaths_}) {
        for (std::set<PathName>::iterator it = fileset->begin(); it != fileset->end();) {

            if (it->exists()) {
                ++it;
            }
            else {
                fileset->erase(it++);
            }
        }
    }

    if (tocPath_.asString().size() && !tocPath_.exists())
        tocPath_ = "";
    if (schemaPath_.asString().size() && !schemaPath_.exists())
        schemaPath_ = "";

    // Consider the total sets of URIs to delete
    // add catalogue URIs
    std::set<eckit::URI> deleteURIs;
    for (const auto& i : indexPaths_) 
        deleteURIs.emplace("file", i);
    for (const auto& s : subtocPaths_) 
        deleteURIs.emplace("file", s);
    for (const auto& l : lockfilePaths_) 
        deleteURIs.emplace("file", l);
    if (tocPath_.asString().size())
        deleteURIs.emplace("file", tocPath_);
    if (schemaPath_.asString().size())
        deleteURIs.emplace("file", schemaPath_);

    // add store URIs
    // if (store_.type() == "file")
    for (const auto& [uri, store] : stores_) {
        auto elements = store->wipeElements();
        if (auto it = elements.find(WipeElementType::WIPE_STORE); it != elements.end()) {
            deleteURIs.insert(it->second.begin(), it->second.end());
        }
        if (auto it = elements.find(WipeElementType::WIPE_STORE_AUX); it != elements.end()) {
            deleteURIs.insert(it->second.begin(), it->second.end());
        }
    }

    // retrieve the total sets of URIs in the catalogue and store
    std::vector<eckit::PathName> allPathsVector;
    StdDir(catalogue_.basePath()).children(allPathsVector);
    // add catalogue URIs

    std::vector<eckit::URI> allCollocatedDataURIs;
    for(const auto& [uri, store] : stores_) {
        auto collocatedDataURIs = store->collocatedDataURIs();
        allCollocatedDataURIs.insert(allCollocatedDataURIs.end(), collocatedDataURIs.begin(), collocatedDataURIs.end());
    }
    allURIs.insert(allCollocatedDataURIs.begin(), allCollocatedDataURIs.end());

    ASSERT(residualURIs_.empty());

    // check if we are about to delete all the existing URIs
    if (!(deleteURIs == allURIs)) {

        // First we check if there are paths marked to delete that don't exist. This is an error

        std::set<eckit::URI> uris;
        std::set_difference(deleteURIs.begin(), deleteURIs.end(), allURIs.begin(), allURIs.end(),
                            std::inserter(uris, uris.begin()));

        if (!uris.empty()) {
            Log::error() << "URIs not in existing URI set:" << std::endl;
            for (const auto& u : uris) {
                Log::error() << " - " << u << std::endl;
            }
            throw SeriousBug(
                "Path to delete should be in existing path set. Are multiple wipe commands running simultaneously?",
                Here());
        }

        std::set_difference(allURIs.begin(), allURIs.end(), deleteURIs.begin(), deleteURIs.end(),
                            std::inserter(residualURIs_, residualURIs_.begin()));
    }

    // // if the store uses a backend other than POSIX (file), repeat the algorithm specialized
    // // for its store units

    // if (store_.type() == "file")
    //     return;

    // std::vector<eckit::PathName> allDataPathsVector;
    // for (const auto& u : allCollocatedDataURIs) {
    //     allDataPathsVector.push_back(eckit::PathName(u.path()));
    // }

    // std::set<eckit::PathName> allDataPaths(allDataPathsVector.begin(), allDataPathsVector.end());

    // ASSERT(residualDataPaths_.empty());

    // if (!(dataPaths_ == allDataPaths)) {

    //     // First we check if there are paths marked to delete that don't exist. This is an error

    //     std::set<PathName> paths;
    //     std::set_difference(dataPaths_.begin(), dataPaths_.end(), allDataPaths.begin(), allDataPaths.end(),
    //                         std::inserter(paths, paths.begin()));

    //     if (!paths.empty()) {
    //         Log::error() << "Store unit paths not in existing paths set:" << std::endl;
    //         for (const auto& p : paths) {
    //             Log::error() << " - " << p << std::endl;
    //         }
    //         throw SeriousBug(
    //             "Store unit path to delete should be in existing path set. Are multiple wipe commands running "
    //             "simultaneously?",
    //             Here());
    //     }

    //     std::set_difference(allDataPaths.begin(), allDataPaths.end(), dataPaths_.begin(), dataPaths_.end(),
    //                         std::inserter(residualDataPaths_, residualDataPaths_.begin()));
    // }
}

bool TocWipeVisitor::anythingToWipe() const {
    bool catalogueToWipe = (!subtocPaths_.empty() || !lockfilePaths_.empty() || !indexPaths_.empty() ||
            !indexesToMask_.empty() || tocPath_.asString().size() || schemaPath_.asString().size());
    if (catalogueToWipe)
        return true;
       
    return false;
}

void TocWipeVisitor::report(bool wipeAll) {

    ASSERT(anythingToWipe());

    std::stringstream ss;
    ss << "FDB owner: " << catalogue_.owner();
    queue_.emplace(WipeElementType::WIPE_CATALOGUE_INFO, ss.str(), std::vector<eckit::URI>{});

    // if (tocPath_.asString().size()) {
    //     queue_.emplace(WIPE_CATALOGUE, "toc", tocPath_);
    // }
    // if (subtocPaths_.size()) {
    //     queue_.emplace(WIPE_CATALOGUE, "subtoc", subtocPaths_);
    // }
    {
        std::vector<eckit::URI> uris;
        if (tocPath_.asString().size()) {
            uris.emplace_back(tocPath_);
        }
        for (const auto& f : subtocPaths_) {
            uris.emplace_back(f);
        }
        queue_.emplace(WipeElementType::WIPE_CATALOGUE, "Toc files to delete:", uris);
    }

    // if (schemaPath_.asString().size()) {
    //     queue_.emplace(WIPE_CATALOGUE_AUX, "schema", schemaPath_);
    // }
    // if (lockfilePaths_.size()) {
    //     queue_.emplace(WIPE_CATALOGUE_AUX, "lock", lockfilePaths_);
    // }
    {
        std::vector<eckit::URI> uris;
        if (schemaPath_.asString().size()) {
            uris.emplace_back(schemaPath_);
        }
        for (const auto& f : lockfilePaths_) {
            uris.emplace_back(f);
        }
        queue_.emplace(WipeElementType::WIPE_CATALOGUE_AUX, "Control files to delete:", uris);
    }

    // if (indexPaths_.size()) {
    //     queue_.emplace(WIPE_CATALOGUE, "index", indexPaths_);
    // }
    {
        std::vector<eckit::URI> uris;
        for (const auto& f : indexPaths_) {
            uris.emplace_back(f);
        }
        queue_.emplace(WipeElementType::WIPE_CATALOGUE, "Index files to delete:", uris);
    }

    // if (dataPaths_.size()) {
    //     queue_.emplace(WIPE_STORE, "data", dataPaths_);
    // }
    for (const auto& [t,uris] : storeWipeElements()) {
        switch (t) {
            case WIPE_STORE:        queue_.emplace(t, "Data files to delete:", uris); break;
            case WIPE_STORE_AUX:    queue_.emplace(t, "Auxiliary files to delete:", uris); break;
            // case WIPE_STORE_SAFE:   queue_.emplace(t, "Protected files (explicitly untouched):", uris); break;
            default:
                std::ostringstream ss;
                ss << "Unexpected WIPE group info " << t << " received ";
                throw eckit::SeriousBug(ss.str(), Here());
        }
    }
    if (wipeAll) {
        std::vector<eckit::URI> storeURIs;
        for (const auto& [uri, store] : stores_) {
            if (uri != catalogue_.uri()) {
                storeURIs.push_back(uri);
            }
        }
        if (!storeURIs.empty()) {
            std::ostringstream ss;
            ss << "Store " << eckit::Plural(storeURIs.size(), "URI") << " to delete:";
            queue_.emplace(WipeElementType::WIPE_STORE_URI, ss.str(), storeURIs);
        }
    }
    if (safeCataloguePaths_.size()) {
        {
            std::vector<eckit::URI> uris;
            for (const auto& f : safeCataloguePaths_) {
                uris.emplace_back(f);
            }
            queue_.emplace(WipeElementType::WIPE_CATALOGUE_SAFE, "Protected files (explicitly untouched):", uris);
        }
        if (indexesToMask_.size()) {
            std::vector<eckit::URI> uris;
            uris.reserve(indexesToMask_.size());
            
            for (const auto& i : indexesToMask_) {
                uris.push_back(i.location().uri());
            }
            queue_.emplace(WipeElementType::WIPE_CATALOGUE_INFO, "Indexes to mask:", uris);
        }
    }
}

void TocWipeVisitor::wipe(bool wipeAll) {

    ASSERT(anythingToWipe());

    std::ostream& logAlways(Log::info());
    std::ostream& logVerbose(/*porcelain_ ? */Log::debug<LibFdb5>() /*: out_*/);

    // Sanity checks...

    catalogue_.checkUID();

    // If we are wiping the metadata files, then we need to lock the DB to ensure we don't get
    // into a state we don't like.

    if (wipeAll && doit_) {
        catalogue_.control(ControlAction::Disable,
                           ControlIdentifier::List | ControlIdentifier::Retrieve | ControlIdentifier::Archive);

        ASSERT(!catalogue_.enabled(ControlIdentifier::List));
        ASSERT(!catalogue_.enabled(ControlIdentifier::Retrieve));
        ASSERT(!catalogue_.enabled(ControlIdentifier::Archive));

        // The lock will have occurred after the visitation phase, so add the lockfiles.
        const auto&& lockfiles(catalogue_.lockfilePaths());
        lockfilePaths_.insert(lockfiles.begin(), lockfiles.end());
    }

    // If we are wiping only a subset, and as a result have indexes to mask out; do that first.
    // This results in a failure mode merely being data becoming invisible (which has the correct
    // effect for the user), to be wiped at a later date.

    if (!indexesToMask_.empty() && !wipeAll) {
        for (const auto& index : indexesToMask_) {
            logVerbose << "Index to mask: ";
            logAlways << index << std::endl;
            if (doit_)
                catalogue_.maskIndexEntry(index);
        }
    }

    // Now we want to do the actual deletion
    // n.b. We delete carefully in a order such that we can always access the DB by what is left

    /// @todo: are all these exist checks necessary?

    for (auto& [uri,store] : stores_) {
        store->doWipe();
    }
    // for (const PathName& path : residualDataPaths_) {
    //     eckit::URI uri(store_.type(), path);
    //     if (store_.uriExists(uri)) {
    //         store_.remove(uri, logAlways, logVerbose, doit_);
    //     }
    // }
    for (const auto& uri : residualURIs_) {
        auto path = uri.path();
        if (path.exists()) {
            catalogue_.remove(path, logAlways, logVerbose, doit_);
        }
    }

    // for (const PathName& path : dataPaths_) {
    //     eckit::URI uri(store_.type(), path);
    //     if (store_.uriExists(uri)) {
    //         store_.remove(uri, logAlways, logVerbose, doit_);
    //     }
    // }

    // for (const PathName& path : auxiliaryDataPaths_) {
    //     eckit::URI uri(store_.type(), path);
    //     if (store_.auxiliaryURIExists(uri)) {
    //         store_.remove(uri, logAlways, logVerbose, doit_);
    //     }
    // }

    // if (wipeAll && store_.type() != "file")
    //     /// @todo: if the store is holding catalogue information (e.g. daos KVs) it
    //     ///    should not be removed
    //     store_.remove(store_.uri(), logAlways, logVerbose, doit_);

    for (const std::set<PathName>& pathset :
         {indexPaths_, std::set<PathName>{schemaPath_}, subtocPaths_, std::set<PathName>{tocPath_}, lockfilePaths_,
          (wipeAll ? std::set<PathName>{catalogue_.basePath()} : std::set<PathName>{})}) {

        for (const PathName& path : pathset) {
            if (path.exists()) {
                catalogue_.remove(path, logAlways, logVerbose, doit_);
            }
        }
    }
}


void TocWipeVisitor::catalogueComplete(const Catalogue& catalogue) {
    WipeVisitor::catalogueComplete(catalogue);

    // We wipe everything if there is nothingn within safePaths - i.e. there is
    // no data that wasn't matched by the request

    bool wipeAll = safeCataloguePaths_.empty();

    if (wipeAll) {
        addMaskedPaths();
        addMetadataPaths();
    }
    else {
        // Ensure we _really_ don't delete these if not wiping everything
        subtocPaths_.clear();
        lockfilePaths_.clear();
        tocPath_    = "";
        schemaPath_ = "";
    }

    ensureSafePaths();

    if (anythingToWipe()) {
        if (wipeAll)
            calculateResidualPaths();

        if (!porcelain_)
            report(wipeAll);

        // This is here as it needs to run whatever combination of doit/porcelain/...
        if (wipeAll && !residualURIs_.empty()) {


            // std::cout << "TO CHANGE " << "Unexpected files present in directory: " << std::endl;
            // for (const auto& u : residualURIs_) std::cout << "TO CHANGE " << "    " << u << std::endl;
            // std::cout << "TO CHANGE " << std::endl;


        //     std::cout << "TO CHANGE "  << "Unexpected store units present in store: " << std::endl;
        //     for (const auto& p : residualDataPaths_) std::cout << "TO CHANGE "  << "    " << store_.type() << "://" << p << std::endl;
        //     std::cout << "TO CHANGE "  << std::endl;

            if (!unsafeWipeAll_) {
                // out_ << "Full wipe will not proceed without --unsafe-wipe-all" << std::endl;
                queue_.emplace(WIPE_CATALOGUE_INFO, "Full wipe will not proceed without --unsafe-wipe-all", std::vector<eckit::URI>{});
                if (doit_)
                    throw Exception("Cannot fully wipe unclean TocDB", Here());
            }
        }

        if (doit_ || porcelain_)
            wipe(wipeAll);
    }
}


//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
