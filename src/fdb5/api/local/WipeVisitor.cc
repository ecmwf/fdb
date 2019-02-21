
/*
 * (C) Copyright 2018- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/api/local/WipeVisitor.h"

#include "fdb5/database/DB.h"
#include "fdb5/database/Index.h"
#include "fdb5/LibFdb.h"

#include "eckit/os/Stat.h"

#include <sys/stat.h>
#include <dirent.h>

namespace fdb5 {
namespace api {
namespace local {

//----------------------------------------------------------------------------------------------------------------------

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
            paths.push_back(p);

            eckit::Stat::Struct info;
            SYSCALL(eckit::Stat::lstat(p.localPath(), &info));

            if (S_ISDIR(info.st_mode)) {
                StdDir d(p);
                d.children(paths);
            }
        }
    }
};

//----------------------------------------------------------------------------------------------------------------------

WipeVisitor::WipeVisitor(eckit::Queue<WipeElement>& queue,
                         const metkit::MarsRequest& request,
                         bool doit,
                         bool verbose) :
    QueryVisitor(queue, request),
    doit_(doit),
    verbose_(verbose) {}

bool WipeVisitor::visitDatabase(const DB& db) {

    EntryVisitor::visitDatabase(db);

    ASSERT(current_.metadataPaths.empty());
    ASSERT(current_.dataPaths.empty());
    ASSERT(current_.otherPaths.empty());
    ASSERT(current_.safePaths.empty());
    ASSERT(indexesToMask_.empty());

    basePath_ = db.basePath();

    // Who owns this DB

    current_.owner = db.owner();

    // Does the request that has got us here entirely select the DB, or
    // is there potential further subselection of Requests

    indexRequest_ = request_;
    for (const auto& kv : db.key()) {
        indexRequest_.unsetValues(kv.first);
    }

    // Enumerate metadata components

    for (const eckit::PathName& path : db.metadataPaths()) {
        if (path.dirName().sameAs(basePath_)) {
            current_.metadataPaths.insert(path);
        }
    }

    return true; // Explore contained indexes
}

bool WipeVisitor::visitIndex(const Index& index) {

    eckit::PathName location(index.location().url());

    // Is this index matched by the supplied request?
    // n.b. If the request is over-specified (i.e. below the index level), nothing will be removed

    bool include = index.key().match(indexRequest_);

    ASSERT(location.dirName().sameAs(basePath_));
    if (include) {
        indexesToMask_.push_back(index);
        current_.indexes.push_back(std::shared_ptr<IndexLocation>(index.location().clone()));
        current_.metadataPaths.insert(location);
    } else {
        current_.safePaths.insert(basePath_);
        for (const eckit::PathName& path : currentDatabase_->metadataPaths()) {
            current_.safePaths.insert(path);
        }
        current_.safePaths.insert(location);
    }

    // Enumerate data files.

    for (const eckit::PathName& path : index.dataPaths()) {
        if (path.dirName().sameAs(basePath_)) {
            if (include) {
                current_.dataPaths.insert(path);
            } else {
                current_.safePaths.insert(path);
            }
        }
    }

    return true; // Explore contained entries
}

void WipeVisitor::databaseComplete(const DB& db) {
    EntryVisitor::databaseComplete(db);

    // Build a list of 'other' paths to include.

    std::vector<eckit::PathName> otherPaths;
    StdDir(basePath_).children(otherPaths);

    for (const eckit::PathName& path : otherPaths) {
        if (current_.metadataPaths.find(path) == current_.metadataPaths.end() &&
            current_.dataPaths.find(path) == current_.dataPaths.end()) {

            current_.otherPaths.insert(path);
        }
    }

    // Subtract the safe paths from the various options.

    for (const eckit::PathName& path : current_.safePaths) {
        current_.metadataPaths.erase(path);
        current_.dataPaths.erase(path);
        current_.otherPaths.erase(path);
    }

    // Add to the asynchronous queue _before_ doing anything (so output is fresh).
    // n.b. use push not emplace so we don't destry the structure before using it.

    queue_.push(current_);

    if (doit_) {

        std::ostream& log(verbose_ ? eckit::Log::info() : eckit::Log::debug<LibFdb>());

        // Sanity check...
        db.checkUID();

        ASSERT(basePath_ == db.basePath());

        // Do masking first, as if things disappear this is a safety risk.

        if (!current_.safePaths.empty() && !indexesToMask_.empty()) {
            for (const auto& index : indexesToMask_) {
                log << "Index to mask: " << index << std::endl;
                db.maskIndexEntry(index);
            }
        }

        // Now remove unused paths

        for (const eckit::PathName& path : current_.metadataPaths) {
            log << "Unlinking: " << path << std::endl;
            path.unlink(verbose_);
        }

        for (const eckit::PathName& path : current_.dataPaths) {
            log << "Unlinking: " << path << std::endl;
            path.unlink(verbose_);
        }

        for (const eckit::PathName& path : current_.otherPaths) {
            if (path.isDir() && !path.isLink()) {
                log << "rmdir: " << path << std::endl;
                path.rmdir(verbose_);
            } else {
                log << "Unlinking: " << path << std::endl;
                path.unlink(verbose_);
            }
        }

        if (basePath_.exists() && current_.safePaths.empty()) {
            ASSERT(basePath_.isDir());
            log << "rmdir: " << basePath_ << std::endl;
            basePath_.rmdir(verbose_);
        }
    }

    // Cleanup counts for all the existant bits

    current_ = WipeElement();
    indexesToMask_.clear();
}




//----------------------------------------------------------------------------------------------------------------------

} // namespace local
} // namespace api
} // namespace fdb5
