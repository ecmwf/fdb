/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/toc/TocWipeVisitor.h"

#include <algorithm>

#include "eckit/os/Stat.h"

#include "fdb5/toc/TocDB.h"
#include "fdb5/api/helpers/ControlIterator.h"

#include <sys/types.h>
#include <dirent.h>
#include <errno.h>
#include <cstring>


using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

namespace {
class StdDir {

    eckit::PathName path_;
    DIR *d_;

public:

    StdDir(const eckit::PathName& p) :
        path_(p),
        d_(opendir(p.localPath())) {

        if (!d_) {
            std::stringstream ss;
            ss << "Failed to open directory " << p << " (" << errno << "): " << strerror(errno);
            throw eckit::SeriousBug(ss.str(), Here());
        }
    }

    ~StdDir() { if (d_) closedir(d_); }

    void children(std::vector<eckit::PathName>& paths) {

        // Implemented here as PathName::match() does not return hidden files starting with '.'

        struct dirent* e;

        while ((e = readdir(d_)) != 0) {

            if (e->d_name[0] == '.') {
                if (e->d_name[1] == '\0' || (e->d_name[1] == '.' && e->d_name[2] == '\0')) continue;
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
}

//----------------------------------------------------------------------------------------------------------------------

// TODO: Warnings and errors form inside here back to the user.

TocWipeVisitor::TocWipeVisitor(const TocDB& db,
                               const metkit::MarsRequest& request,
                               std::ostream& out,
                               bool doit,
                               bool porcelain,
                               bool unsafeWipeAll) :
    WipeVisitor(request, out, doit, porcelain, unsafeWipeAll),
    db_(db),
    tocPath_(""),
    schemaPath_("") {}

TocWipeVisitor::~TocWipeVisitor() {}


bool TocWipeVisitor::visitDatabase(const DB& db) {

    // Overall checks

    ASSERT(&db_ == &db);
    ASSERT(!db.wipeLocked());
    WipeVisitor::visitDatabase(db);

    // Check that we are in a clean state (i.e. we only visit one DB).

    ASSERT(subtocPaths_.empty());
    ASSERT(lockfilePaths_.empty());
    ASSERT(indexPaths_.empty());
    ASSERT(dataPaths_.empty());
    ASSERT(safePaths_.empty());
    ASSERT(indexesToMask_.empty());

    ASSERT(!tocPath_.asString().size());
    ASSERT(!schemaPath_.asString().size());

    // Having selected a DB, construct the residual request. This is the request that is used for
    // matching Index(es) -- which is relevant if there is subselection of the DB.

    indexRequest_ = request_;
    for (const auto& kv : db.key()) {
        indexRequest_.unsetValues(kv.first);
    }

    return true; // Explore contained indexes
}

bool TocWipeVisitor::visitIndex(const Index& index) {

    eckit::PathName location(index.location().url());
    const auto& basePath(db_.basePath());

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
    } else {
        // This will ensure that if only some indexes are to be removed from a file, then
        // they will be masked out but the file not deleted.
        safePaths_.insert(location);
    }

    // Enumerate data files.

    std::vector<eckit::PathName> indexDataPaths(index.dataPaths());
    for (const eckit::PathName& path : indexDataPaths) {
        if (include && path.dirName().sameAs(basePath)) {
            dataPaths_.insert(path);
        } else {
            safePaths_.insert(path);
        }
    }

    return true; // Explore contained entries
}

void TocWipeVisitor::addMaskedPaths() {

    std::set<std::pair<eckit::PathName, Offset>> metadata;
    std::set<eckit::PathName> data;
    db_.allMasked(metadata, data);
    for (const auto& entry : metadata) {
        if (entry.first.dirName().sameAs(db_.basePath())) {
            if (entry.first.baseName().asString().substr(0, 4) == "toc.") {
                subtocPaths_.insert(entry.first);
            } else {
                indexPaths_.insert(entry.first);
            }
        }
    }
    for (const auto& path : data) {
        if (path.dirName().sameAs(db_.basePath())) dataPaths_.insert(path);
    }
}

void TocWipeVisitor::addMetadataPaths() {

    // toc, schema

    schemaPath_ = db_.schemaPath();
    tocPath_ = db_.tocPath();

    // subtocs

    const auto&& subtocs(db_.subTocPaths());
    subtocPaths_.insert(subtocs.begin(), subtocs.end());

    // lockfiles

    const auto&& lockfiles(db_.lockfilePaths());
    lockfilePaths_.insert(lockfiles.begin(), lockfiles.end());
}

void TocWipeVisitor::ensureSafePaths() {

    // Very explicitly ensure that we cannot delete anything marked as safe

    if (safePaths_.find(tocPath_) != safePaths_.end()) tocPath_ = "";
    if (safePaths_.find(schemaPath_) != safePaths_.end()) schemaPath_ = "";

    for (const auto& p : safePaths_) {
        for (std::set<PathName>* s : {&subtocPaths_, &lockfilePaths_, &indexPaths_, &dataPaths_}) {
            s->erase(p);
        }
    }
}
void TocWipeVisitor::calculateResidualPaths() {

    // Remove paths to non-existant files. This is reasonable as we may be recovering from a
    // previous failed, partial wipe. As such, referenced files may not exist any more.

    for (std::set<PathName>* fileset : {&subtocPaths_, &lockfilePaths_, &indexPaths_, &dataPaths_}) {
        for (std::set<PathName>::iterator it = fileset->begin(); it != fileset->end(); ) {
            if (it->exists()) {
                ++it;
            } else {
                fileset->erase(it++);
            }
        }
    }
    if (tocPath_.asString().size() && !tocPath_.exists()) tocPath_ = "";
    if (schemaPath_.asString().size() && !schemaPath_.exists()) schemaPath_ = "";

    // Consider the total sets of paths

    std::set<eckit::PathName> deletePaths;
    deletePaths.insert(subtocPaths_.begin(), subtocPaths_.end());
    deletePaths.insert(lockfilePaths_.begin(), lockfilePaths_.end());
    deletePaths.insert(indexPaths_.begin(), indexPaths_.end());
    deletePaths.insert(dataPaths_.begin(), dataPaths_.end());
    if (tocPath_.asString().size()) deletePaths.insert(tocPath_);
    if (schemaPath_.asString().size()) deletePaths.insert(schemaPath_);

    std::vector<eckit::PathName> allPathsVector;
    StdDir(db_.basePath()).children(allPathsVector);
    std::set<eckit::PathName> allPaths(allPathsVector.begin(), allPathsVector.end());

    ASSERT(residualPaths_.empty());

    if (deletePaths != allPaths) {

        // First we check if there are paths marked to delete that don't exist. This is an error

        std::set<PathName> paths;
        std::set_difference(deletePaths.begin(), deletePaths.end(),
                            allPaths.begin(), allPaths.end(),
                            std::inserter(paths, paths.begin()));

        if (!paths.empty()) {
	    Log::error() << "Paths not in existing paths set:" << std::endl;
	    for (const auto& p : paths) {
		Log::error() << " - " << p << std::endl;
	    }
            throw SeriousBug("Path to delete should be in existing path set. Are multiple wipe commands running simultaneously?", Here());
        }

        std::set_difference(allPaths.begin(), allPaths.end(),
                            deletePaths.begin(), deletePaths.end(),
                            std::inserter(residualPaths_, residualPaths_.begin()));
    }
}

bool TocWipeVisitor::anythingToWipe() const {
    return (!subtocPaths_.empty() || !lockfilePaths_.empty() || !indexPaths_.empty() ||
            !dataPaths_.empty() || !indexesToMask_.empty() ||
            tocPath_.asString().size() || schemaPath_.asString().size());
}

void TocWipeVisitor::report() {

    ASSERT(anythingToWipe());

    out_ << "FDB owner: " << db_.owner() << std::endl
         << std::endl;

    out_ << "Toc files to delete:" << std::endl;
    if (!tocPath_.asString().size() && subtocPaths_.empty()) out_ << " - NONE -" << std::endl;
    if (tocPath_.asString().size()) out_ << "    " << tocPath_ << std::endl;
    for (const auto& f : subtocPaths_) {
        out_ << "    " << f << std::endl;
    }
    out_ << std::endl;

    out_ << "Control files to delete:" << std::endl;
    if (!schemaPath_.asString().size() && lockfilePaths_.empty()) out_ << " - NONE -" << std::endl;
    if (schemaPath_.asString().size()) out_ << "    " << schemaPath_ << std::endl;
    for (const auto& f : lockfilePaths_) {
        out_ << "    " << f << std::endl;
    }
    out_ << std::endl;

    out_ << "Index files to delete: " << std::endl;
    if (indexPaths_.empty()) out_ << " - NONE -" << std::endl;
    for (const auto& f : indexPaths_) {
        out_ << "    " << f << std::endl;
    }
    out_ << std::endl;

    out_ << "Data files to delete: " << std::endl;
    if (dataPaths_.empty()) out_ << " - NONE -" << std::endl;
    for (const auto& f : dataPaths_) {
        out_ << "    " << f << std::endl;
    }
    out_ << std::endl;

    out_ << "Protected files (explicitly untouched):" << std::endl;
    if (safePaths_.empty()) out_ << " - NONE - " << std::endl;
    for (const auto& f : safePaths_) {
        out_ << "    " << f << std::endl;
    }
    out_ << std::endl;

    if (!safePaths_.empty()) {
        out_ << "Indexes to mask:" << std::endl;
        if (indexesToMask_.empty()) out_ << " - NONE - " << std::endl;
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

    db_.checkUID();

    // If we are wiping the metadata files, then we need to lock the DB to ensure we don't get
    // into a state we don't like.

    if (wipeAll && doit_) {
        db_.control(ControlAction::Lock, ControlIdentifier::List |
                                         ControlIdentifier::Retrieve |
                                         ControlIdentifier::Archive);

        ASSERT(db_.listLocked());
        ASSERT(db_.retrieveLocked());
        ASSERT(db_.archiveLocked());

        // The lock will have occurred after the visitation phase, so add the lockfiles.
        const auto&& lockfiles(db_.lockfilePaths());
        lockfilePaths_.insert(lockfiles.begin(), lockfiles.end());
    }

    // If we are wiping only a subset, and as a result have indexes to mask out; do that first.
    // This results in a failure mode merely being data becoming invisible (which has the correct
    // effect for the user), to be wiped at a later date.

    if (!indexesToMask_.empty() && !wipeAll) {
        for (const auto& index : indexesToMask_) {
            logVerbose << "Index to mask: ";
            logAlways << index << std::endl;
            if (doit_) db_.maskIndexEntry(index);
        }
    }

    // Now we want to do the actual deletion
    // n.b. We delete carefully in a order such that we can always access the DB by what is left

    for (const std::set<PathName>& pathset : {residualPaths_, dataPaths_, indexPaths_,
                                              std::set<PathName>{schemaPath_}, subtocPaths_,
                                              std::set<PathName>{tocPath_}, lockfilePaths_,
                                              (wipeAll ? std::set<PathName>{db_.basePath()} : std::set<PathName>{})}) {

        for (const PathName& path : pathset) {
            if (path.exists()) {
                if (path.isDir()) {
                    logVerbose << "rmdir: ";
                    logAlways << path << std::endl;
                    if (doit_) path.rmdir(false);
                } else {
                    logVerbose << "Unlinking: ";
                    logAlways << path << std::endl;
                    if (doit_) path.unlink(false);
                }
            }

        }
    }
}


void TocWipeVisitor::databaseComplete(const DB& db) {
    WipeVisitor::databaseComplete(db);

    // We wipe everything if there is nothingn within safePaths - i.e. there is
    // no data that wasn't matched by the request

    bool wipeAll = safePaths_.empty();

    if (wipeAll) {
        addMaskedPaths();
        addMetadataPaths();
    } else {
        // Ensure we _really_ don't delete these if not wiping everything
        subtocPaths_.clear();
        lockfilePaths_.clear();
        tocPath_ = "";
        schemaPath_ = "";
    }

    ensureSafePaths();

    if (anythingToWipe()) {
        if (wipeAll) calculateResidualPaths();

        if (!porcelain_) report();

        // This is here as it needs to run whatever combination of doit/porcelain/...
        if (wipeAll && !residualPaths_.empty()) {

            out_ << "Unexpected files present in directory: " << std::endl;
            for (const auto& p : residualPaths_) out_ << "    " << p << std::endl;
            out_ << std::endl;

            if (!unsafeWipeAll_) {
                out_ << "Full wipe will not proceed without --unsafe-wipe-all" << std::endl;
                if (doit_)
                    throw Exception("Cannot fully wipe unclean TocDB", Here());
            }
        }

        if (doit_ || porcelain_) wipe(wipeAll);
    }
}


//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
