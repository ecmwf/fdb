
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

#include "fdb5/api/local/QueueStringLogTarget.h"
#include "fdb5/database/DB.h"
#include "fdb5/database/Index.h"
#include "fdb5/LibFdb5.h"

#include "eckit/os/Stat.h"

#include <sys/stat.h>
#include <dirent.h>

using namespace eckit;

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
                         bool porcelain) :
    QueryVisitor<WipeElement>(queue, request),
    out_(new QueueStringLogTarget(queue)),
    doit_(doit),
    porcelain_(porcelain) {}

bool WipeVisitor::visitDatabase(const DB& db) {

    EntryVisitor::visitDatabase(db);

    ASSERT(metadataPaths_.empty());
    ASSERT(dataPaths_.empty());
    ASSERT(safePaths_.empty());
    ASSERT(indexesToMask_.empty());

    basePath_ = db.basePath();

    // Who owns this DB

    owner_ = db.owner();

    // Does the request that has got us here entirely select the DB, or
    // is there potential further subselection of Requests

    indexRequest_ = request_;
    for (const auto& kv : db.key()) {
        indexRequest_.unsetValues(kv.first);
    }

    // Enumerate metadata components

    for (const eckit::PathName& path : db.metadataPaths()) {
        if (path.dirName().sameAs(basePath_)) {
            metadataPaths_.insert(path);
        }
    }

    // Enumerate masked stuff to be removed if, and only if, the request exactly
    // matches the DB (i.e. everything should be removed).

    if (db.key().match(request_)) {

        ASSERT(indexRequest_.empty());

        std::set<std::pair<eckit::PathName, Offset>> metadata;
        std::set<eckit::PathName> data;
        db.allMasked(metadata, data);
        for (const auto& entry : metadata) {
            if (entry.first.dirName().sameAs(basePath_)) metadataPaths_.insert(entry.first);
        }
        for (const auto& path : data) {
            if (path.dirName().sameAs(basePath_)) dataPaths_.insert(path);
        }
    }

    return true; // Explore contained indexes
}

bool WipeVisitor::visitIndex(const Index& index) {

    eckit::PathName location(index.location().url());

    // Is this index matched by the supplied request?
    // n.b. If the request is over-specified (i.e. below the index level), nothing will be removed

    bool include = index.key().match(indexRequest_);

    // If we have cross fdb-mounted another DB, ensure we can't delete another
    // DBs data.
    if (!location.dirName().sameAs(basePath_)) {
        include = false;
    }

    ASSERT(location.dirName().sameAs(basePath_) || !include);
    if (include) {
        indexesToMask_.push_back(index);
        metadataPaths_.insert(location);
    } else {
        safePaths_.insert(basePath_);
        for (const eckit::PathName& path : currentDatabase_->metadataPaths()) {
            safePaths_.insert(path);
        }
        safePaths_.insert(location);
    }

    // Enumerate data files.

    for (const eckit::PathName& path : index.dataPaths()) {
        if (include && path.dirName().sameAs(basePath_)) {
            dataPaths_.insert(path);
        } else {
            safePaths_.insert(path);
        }
    }

    return true; // Explore contained entries
}

void WipeVisitor::report() {

    out_ << "FDB owner: " << owner_ << std::endl
         << std::endl;

    out_ << "Metadata files to delete:" << std::endl;
    if (dataPaths_.empty()) out_ << " - NONE -" << std::endl;
    for (const auto& f : metadataPaths_) {
        out_ << "    " << f << std::endl;
    }
    out_ << std::endl;

    out_ << "Data files to delete: " << std::endl;
    if (dataPaths_.empty()) out_ << " - NONE -" << std::endl;
    for (const auto& f : dataPaths_) {
        out_ << "    " << f << std::endl;
    }
    out_ << std::endl;

    out_ << "Untouched files:" << std::endl;
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

void WipeVisitor::wipe(const DB& db) {

    std::ostream& logAlways(out_);
    std::ostream& logVerbose(porcelain_ ? Log::debug<LibFdb5>() : out_);

    // Sanity check...
    db.checkUID();

    ASSERT(basePath_ == db.basePath());

    // Do masking first, as if things disappear this is a safety risk.

    if (!safePaths_.empty() && !indexesToMask_.empty()) {
        for (const auto& index : indexesToMask_) {
            logVerbose << "Index to mask: ";
            logAlways << index << std::endl;
            if (doit_) db.maskIndexEntry(index);
        }
    }

    // Now remove unused paths

    for (const eckit::PathName& path : metadataPaths_) {
        logVerbose << "Unlinking: ";
        logAlways << path << std::endl;
        if (doit_) path.unlink(false);
    }

    for (const eckit::PathName& path : dataPaths_) {
        logVerbose << "Unlinking: ";
        logAlways << path << std::endl;
        if (doit_) path.unlink(false);
    }

    if (basePath_.exists() && safePaths_.empty()) {
        ASSERT(basePath_.isDir());
        logVerbose << "rmdir: ";
        logAlways << basePath_ << std::endl;
        if (doit_) basePath_.rmdir(false);
    }
}


void WipeVisitor::databaseComplete(const DB& db) {
    EntryVisitor::databaseComplete(db);

    // Subtract the safe paths from the various options.

    for (const eckit::PathName& path : safePaths_) {
        metadataPaths_.erase(path);
        dataPaths_.erase(path);
    }

    // If there is nothing found, then don't do anything that will write to stdout,
    // or do any work

    if (!metadataPaths_.empty() || !dataPaths_.empty() || !indexesToMask_.empty()) {

        // Report and do

        if (!porcelain_) report();

        if (doit_ || porcelain_) {
            wipe(db);
        }
    }

    // Cleanup counts for all the existant bits

    owner_ = {};
    metadataPaths_.clear();
    dataPaths_.clear();
    safePaths_.clear();
    indexesToMask_.clear();
}




//----------------------------------------------------------------------------------------------------------------------

} // namespace local
} // namespace api
} // namespace fdb5
