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

#include "eckit/os/Stat.h"

#include "fdb5/api/helpers/ControlIterator.h"
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
        if (d_) {
            closedir(d_);
        }
    }

    void children(std::vector<eckit::PathName>& paths) {

        // Implemented here as PathName::match() does not return hidden files starting with '.'

        struct dirent* e;

        while ((e = readdir(d_)) != nullptr) {

            if (e->d_name[0] == '.') {
                if (e->d_name[1] == '\0' || (e->d_name[1] == '.' && e->d_name[2] == '\0')) {
                    continue;
                }
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

TocWipeVisitor::TocWipeVisitor(const TocCatalogue& catalogue, const Store& store,
                               const metkit::mars::MarsRequest& request, std::ostream& out, bool doit, bool porcelain,
                               bool unsafeWipeAll) :
    WipeVisitor(request, out, doit, porcelain, unsafeWipeAll),
    catalogue_(catalogue),
    store_(store),
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
    ASSERT(dataPaths_.empty());
    ASSERT(auxiliaryDataPaths_.empty());
    ASSERT(safePaths_.empty());
    ASSERT(indexesToMask_.empty());

    ASSERT(!tocPath_.asString().size());
    ASSERT(!schemaPath_.asString().size());

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
        safePaths_.insert(location);
    }

    // Enumerate data files.

    std::vector<eckit::URI> indexDataPaths(index.dataURIs());
    auto paths = store_.asCollocatedDataURIs(indexDataPaths);

    for (const eckit::URI& uri : store_.asCollocatedDataURIs(indexDataPaths)) {
        if (include) {
            if (!store_.uriBelongs(uri)) {
                Log::error() << "Index to be deleted has pointers to fields that don't belong to the configured store."
                             << std::endl;
                Log::error() << "Configured Store URI: " << store_.uri().asString() << std::endl;
                Log::error() << "Pointed Store unit URI: " << uri.asString() << std::endl;
                Log::error() << "Impossible to delete such fields. Index deletion aborted to avoid leaking fields."
                             << std::endl;
                NOTIMP;
            }
            dataPaths_.insert(eckit::PathName(uri.path()));
            auto auxPaths = getAuxiliaryPaths(uri);
            auxiliaryDataPaths_.insert(auxPaths.begin(), auxPaths.end());
        }
        else {
            safePaths_.insert(eckit::PathName(uri.path()));
            if (store_.uriBelongs(uri)) {
                auto auxPaths = getAuxiliaryPaths(uri);
                safePaths_.insert(auxPaths.begin(), auxPaths.end());
            }
        }
    }

    return true;  // Explore contained entries
}

std::vector<eckit::PathName> TocWipeVisitor::getAuxiliaryPaths(const eckit::URI& dataURI) {
    // todo: in future, we should be using URIs, not paths.
    std::vector<eckit::PathName> paths;
    for (const auto& auxURI : store_.getAuxiliaryURIs(dataURI)) {
        if (store_.auxiliaryURIExists(auxURI)) {
            paths.push_back(auxURI.path());
        }
    }
    return paths;
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
    for (const auto& uri : data) {
        if (store_.uriBelongs(uri)) {
            dataPaths_.insert(uri.path());
            auto auxPaths = getAuxiliaryPaths(uri);
            auxiliaryDataPaths_.insert(auxPaths.begin(), auxPaths.end());
        }
    }
}

void TocWipeVisitor::addMetadataPaths() {

    // toc, schema

    schemaPath_ = catalogue_.schemaPath().path();
    tocPath_ = catalogue_.tocPath().path();

    // subtocs

    const auto&& subtocs(catalogue_.subTocPaths());
    subtocPaths_.insert(subtocs.begin(), subtocs.end());

    // lockfiles

    const auto&& lockfiles(catalogue_.lockfilePaths());
    lockfilePaths_.insert(lockfiles.begin(), lockfiles.end());
}

void TocWipeVisitor::ensureSafePaths() {

    // Very explicitly ensure that we cannot delete anything marked as safe

    if (safePaths_.find(tocPath_) != safePaths_.end()) {
        tocPath_ = "";
    }
    if (safePaths_.find(schemaPath_) != safePaths_.end()) {
        schemaPath_ = "";
    }

    for (const auto& p : safePaths_) {
        for (std::set<PathName>* s :
             {&subtocPaths_, &lockfilePaths_, &indexPaths_, &dataPaths_, &auxiliaryDataPaths_}) {
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

    for (std::set<PathName>* fileset : {&dataPaths_}) {
        for (std::set<PathName>::iterator it = fileset->begin(); it != fileset->end();) {

            if (store_.uriExists(eckit::URI(store_.type(), *it))) {
                ++it;
            }
            else {
                fileset->erase(it++);
            }
        }
    }

    if (tocPath_.asString().size() && !tocPath_.exists()) {
        tocPath_ = "";
    }

    if (schemaPath_.asString().size() && !schemaPath_.exists()) {
        schemaPath_ = "";
    }

    // Consider the total sets of paths

    std::set<eckit::PathName> deletePaths;
    deletePaths.insert(subtocPaths_.begin(), subtocPaths_.end());
    deletePaths.insert(lockfilePaths_.begin(), lockfilePaths_.end());
    deletePaths.insert(indexPaths_.begin(), indexPaths_.end());
    if (store_.type() == "file") {
        deletePaths.insert(dataPaths_.begin(), dataPaths_.end());
    }
    if (tocPath_.asString().size()) {
        deletePaths.insert(tocPath_);
    }
    if (schemaPath_.asString().size()) {
        deletePaths.insert(schemaPath_);
    }

    deletePaths.insert(auxiliaryDataPaths_.begin(), auxiliaryDataPaths_.end());
    std::vector<eckit::PathName> allPathsVector;
    StdDir(catalogue_.basePath()).children(allPathsVector);
    std::set<eckit::PathName> allPaths(allPathsVector.begin(), allPathsVector.end());

    ASSERT(residualPaths_.empty());

    if (!(deletePaths == allPaths)) {

        // First we check if there are paths marked to delete that don't exist. This is an error

        std::set<PathName> paths;
        std::set_difference(deletePaths.begin(), deletePaths.end(), allPaths.begin(), allPaths.end(),
                            std::inserter(paths, paths.begin()));

        if (!paths.empty()) {
            Log::error() << "Paths not in existing paths set:" << std::endl;
            for (const auto& p : paths) {
                Log::error() << " - " << p << std::endl;
            }
            throw SeriousBug(
                "Path to delete should be in existing path set. Are multiple wipe commands running simultaneously?",
                Here());
        }

        std::set_difference(allPaths.begin(), allPaths.end(), deletePaths.begin(), deletePaths.end(),
                            std::inserter(residualPaths_, residualPaths_.begin()));
    }

    // if the store uses a backend other than POSIX (file), repeat the algorithm specialized
    // for its store units

    if (store_.type() == "file") {
        return;
    }

    std::vector<eckit::URI> allCollocatedDataURIs(store_.collocatedDataURIs());
    std::vector<eckit::PathName> allDataPathsVector;
    for (const auto& u : allCollocatedDataURIs) {
        allDataPathsVector.push_back(eckit::PathName(u.path()));
    }

    std::set<eckit::PathName> allDataPaths(allDataPathsVector.begin(), allDataPathsVector.end());

    ASSERT(residualDataPaths_.empty());

    if (!(dataPaths_ == allDataPaths)) {

        // First we check if there are paths marked to delete that don't exist. This is an error

        std::set<PathName> paths;
        std::set_difference(dataPaths_.begin(), dataPaths_.end(), allDataPaths.begin(), allDataPaths.end(),
                            std::inserter(paths, paths.begin()));

        if (!paths.empty()) {
            Log::error() << "Store unit paths not in existing paths set:" << std::endl;
            for (const auto& p : paths) {
                Log::error() << " - " << p << std::endl;
            }
            throw SeriousBug(
                "Store unit path to delete should be in existing path set. Are multiple wipe commands running "
                "simultaneously?",
                Here());
        }

        std::set_difference(allDataPaths.begin(), allDataPaths.end(), dataPaths_.begin(), dataPaths_.end(),
                            std::inserter(residualDataPaths_, residualDataPaths_.begin()));
    }
}

bool TocWipeVisitor::anythingToWipe() const {
    return (!subtocPaths_.empty() || !lockfilePaths_.empty() || !indexPaths_.empty() || !dataPaths_.empty() ||
            !indexesToMask_.empty() || tocPath_.asString().size() || schemaPath_.asString().size());
}

void TocWipeVisitor::report(bool wipeAll) {

    ASSERT(anythingToWipe());

    out_ << "FDB owner: " << catalogue_.owner() << std::endl << std::endl;

    out_ << "Toc files to delete:" << std::endl;
    if (!tocPath_.asString().size() && subtocPaths_.empty()) {
        out_ << " - NONE -" << std::endl;
    }
    if (tocPath_.asString().size()) {
        out_ << "    " << tocPath_ << std::endl;
    }
    for (const auto& f : subtocPaths_) {
        out_ << "    " << f << std::endl;
    }
    out_ << std::endl;

    out_ << "Control files to delete:" << std::endl;
    if (!schemaPath_.asString().size() && lockfilePaths_.empty()) {
        out_ << " - NONE -" << std::endl;
    }
    if (schemaPath_.asString().size()) {
        out_ << "    " << schemaPath_ << std::endl;
    }
    for (const auto& f : lockfilePaths_) {
        out_ << "    " << f << std::endl;
    }
    out_ << std::endl;

    out_ << "Index files to delete: " << std::endl;
    if (indexPaths_.empty()) {
        out_ << " - NONE -" << std::endl;
    }
    for (const auto& f : indexPaths_) {
        out_ << "    " << f << std::endl;
    }
    out_ << std::endl;

    out_ << "Data files to delete: " << std::endl;
    if (dataPaths_.empty()) {
        out_ << " - NONE -" << std::endl;
    }
    for (const auto& f : dataPaths_) {
        out_ << "    " << f << std::endl;
    }
    out_ << std::endl;

    out_ << "Auxiliary files to delete: " << std::endl;
    if (auxiliaryDataPaths_.empty()) {
        out_ << " - NONE -" << std::endl;
    }
    for (const auto& f : auxiliaryDataPaths_) {
        out_ << "    " << f << std::endl;
    }
    out_ << std::endl;

    if (store_.type() != "file") {
        out_ << "Store URI to delete:" << std::endl;
        if (wipeAll) {
            out_ << "    " << store_.uri() << std::endl;
        }
        else {
            out_ << " - NONE -" << std::endl;
        }
        out_ << std::endl;
    }

    out_ << "Protected files (explicitly untouched):" << std::endl;
    if (safePaths_.empty()) {
        out_ << " - NONE - " << std::endl;
    }
    for (const auto& f : safePaths_) {
        out_ << "    " << f << std::endl;
    }
    out_ << std::endl;

    if (!safePaths_.empty()) {
        out_ << "Indexes to mask:" << std::endl;
        if (indexesToMask_.empty()) {
            out_ << " - NONE - " << std::endl;
        }
        for (const auto& i : indexesToMask_) {
            out_ << "    " << i.location() << std::endl;
        }
    }
}

void TocWipeVisitor::wipe(bool wipeAll) {

    ASSERT(anythingToWipe());

    std::ostream& logAlways(out_);
    std::ostream& logVerbose(porcelain_ ? Log::debug<LibFdb5>() : out_);

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
            if (doit_) {
                catalogue_.maskIndexEntry(index);
            }
        }
    }

    // Now we want to do the actual deletion
    // n.b. We delete carefully in a order such that we can always access the DB by what is left

    /// @todo: are all these exist checks necessary?

    for (const PathName& path : residualDataPaths_) {
        eckit::URI uri(store_.type(), path);
        if (store_.uriExists(uri)) {
            store_.remove(uri, logAlways, logVerbose, doit_);
        }
    }
    for (const PathName& path : residualPaths_) {
        if (path.exists()) {
            catalogue_.remove(path, logAlways, logVerbose, doit_);
        }
    }

    for (const PathName& path : dataPaths_) {
        eckit::URI uri(store_.type(), path);
        if (store_.uriExists(uri)) {
            store_.remove(uri, logAlways, logVerbose, doit_);
        }
    }

    for (const PathName& path : auxiliaryDataPaths_) {
        eckit::URI uri(store_.type(), path);
        if (store_.auxiliaryURIExists(uri)) {
            store_.remove(uri, logAlways, logVerbose, doit_);
        }
    }

    if (wipeAll && store_.type() != "file") {
        /// @todo: if the store is holding catalogue information (e.g. daos KVs) it
        ///    should not be removed
        store_.remove(store_.uri(), logAlways, logVerbose, doit_);
    }

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

    bool wipeAll = safePaths_.empty();

    if (wipeAll) {
        addMaskedPaths();
        addMetadataPaths();
    }
    else {
        // Ensure we _really_ don't delete these if not wiping everything
        subtocPaths_.clear();
        lockfilePaths_.clear();
        tocPath_ = "";
        schemaPath_ = "";
    }

    ensureSafePaths();

    if (anythingToWipe()) {
        if (wipeAll) {
            calculateResidualPaths();
        }

        if (!porcelain_) {
            report(wipeAll);
        }

        // This is here as it needs to run whatever combination of doit/porcelain/...
        if (wipeAll && !residualPaths_.empty()) {

            out_ << "Unexpected files present in directory: " << std::endl;
            for (const auto& p : residualPaths_) {
                out_ << "    " << p << std::endl;
            }
            out_ << std::endl;
        }
        if (wipeAll && !residualDataPaths_.empty()) {

            out_ << "Unexpected store units present in store: " << std::endl;
            for (const auto& p : residualDataPaths_) {
                out_ << "    " << store_.type() << "://" << p << std::endl;
            }
            out_ << std::endl;
        }
        if (wipeAll && (!residualPaths_.empty() || !residualDataPaths_.empty())) {
            if (!unsafeWipeAll_) {
                out_ << "Full wipe will not proceed without --unsafe-wipe-all" << std::endl;
                if (doit_) {
                    throw Exception("Cannot fully wipe unclean TocDB", Here());
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
