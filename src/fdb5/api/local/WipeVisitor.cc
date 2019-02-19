
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

WipeVisitor::WipeVisitor(eckit::Queue<WipeElement> &queue, bool doit, bool verbose) :
    QueryVisitor(queue),
    doit_(doit),
    verbose_(verbose) {}

void WipeVisitor::visitDatabase(const DB& db) {

    EntryVisitor::visitDatabase(db);

    ASSERT(current_.metadataPaths.empty());
    ASSERT(current_.dataPaths.empty());
    ASSERT(current_.otherPaths.empty());

    basePath_ = db.basePath();

    // Who owns this DB

    current_.owner = db.owner();

    // Enumerate metadata components

    for (const eckit::PathName& path : db.metadataPaths()) {
        if (path.dirName().sameAs(basePath_)) {
            current_.metadataPaths.insert(path);
        }
    }
}

void WipeVisitor::visitIndex(const Index& index) {

    eckit::PathName location(index.location().url());

    ASSERT(location.dirName().sameAs(basePath_));
    current_.metadataPaths.insert(location);

    // Enumerate data files.

    for (const eckit::PathName& path : index.dataPaths()) {
        if (path.dirName().sameAs(basePath_)) {
            current_.dataPaths.insert(path);
        }
    }
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

    // Add to the asynchronous queue _before_ doing anything (so output is fresh).
    // n.b. use push not emplace so we don't destry the structure before using it.

    queue_.push(current_);

    if (doit_) {

        // Sanity check...
        db.checkUID();

        ASSERT(basePath_ == db.basePath());

        for (const eckit::PathName& path : current_.metadataPaths) {
            eckit::Log::info() << "Unlinking: " << path << std::endl;
            path.unlink(verbose_);
        }

        for (const eckit::PathName& path : current_.dataPaths) {
            eckit::Log::info() << "Unlinking: " << path << std::endl;
            path.unlink(verbose_);
        }

        for (const eckit::PathName& path : current_.otherPaths) {
            if (path.isDir() && !path.isLink()) {
                eckit::Log::info() << "RMdir: " << path << std::endl;
                path.rmdir(verbose_);
            } else {
                eckit::Log::info() << "Unlinking: " << path << std::endl;
                path.unlink(verbose_);
            }
        }

        if (basePath_.exists()) {
            ASSERT(basePath_.isDir());
            eckit::Log::info() << "rmdir: " << basePath_ << std::endl;
            basePath_.rmdir();
        }
    }

    // Cleanup counts for all the existant bits

    current_ = WipeElement();
}




//----------------------------------------------------------------------------------------------------------------------

} // namespace local
} // namespace api
} // namespace fdb5
