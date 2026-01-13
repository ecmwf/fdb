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
#include <fcntl.h>
#include <algorithm>

#include "eckit/config/Resource.h"
#include "eckit/distributed/Message.h"
#include "eckit/os/Stat.h"

#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Store.h"
#include "fdb5/toc/RootManager.h"
#include "fdb5/toc/TocCatalogue.h"
#include "fdb5/toc/TocMoveVisitor.h"

#include <dirent.h>
#include <fdb5/LibFdb5.h>
#include <sys/file.h>
#include <sys/types.h>
#include <cstring>

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

// TODO: Warnings and errors form inside here back to the user.

TocMoveVisitor::TocMoveVisitor(const TocCatalogue& catalogue, const Store& store,
                               const metkit::mars::MarsRequest& request, const eckit::URI& dest,
                               eckit::Queue<MoveElement>& queue) :
    MoveVisitor(request, dest), catalogue_(catalogue), store_(store), queue_(queue) {}

TocMoveVisitor::~TocMoveVisitor() {}

bool TocMoveVisitor::visitDatabase(const Catalogue& catalogue) {

    // Overall checks
    ASSERT(&catalogue_ == &catalogue);

    MoveVisitor::visitDatabase(catalogue_);

    // TOC specific checks: index files not locked
    DIR* dirp = ::opendir(catalogue_.basePath().c_str());
    struct dirent* dp;
    while ((dp = readdir(dirp)) != NULL) {
        if (strstr(dp->d_name, ".index")) {
            eckit::PathName src_ = PathName(catalogue_.basePath()) / dp->d_name;
            int fd               = ::open(src_.asString().c_str(), O_RDWR);
            if (::flock(fd, LOCK_EX)) {
                std::ostringstream ss;
                ss << "Index file " << dp->d_name << " is locked";
                throw eckit::UserError(ss.str(), Here());
            }
        }
    }
    closedir(dirp);

    // TOC specific checks: destination path suitable for requested key
    if (dest_.scheme().empty() || dest_.scheme() == "toc" || dest_.scheme() == "file" || dest_.scheme() == "unix") {
        eckit::PathName destPath = dest_.path();

        bool found = false;

        for (const eckit::PathName& root : CatalogueRootManager(catalogue_.config()).canMoveToRoots(catalogue_.key())) {

            if (root.sameAs(destPath)) {
                eckit::PathName dest_db = destPath / catalogue_.basePath().baseName(true);

                if (dest_db.exists()) {
                    std::ostringstream ss;
                    ss << "Target folder already exist!" << std::endl;
                    throw UserError(ss.str(), Here());
                }
                found = true;
                break;
            }
        }
        if (!found) {
            std::ostringstream ss;
            ss << "Destination " << dest_ << " cannot be used to archive a DB with key: " << catalogue_.key()
               << std::endl;
            throw eckit::UserError(ss.str(), Here());
        }
    }
    else {
        std::ostringstream ss;
        ss << "Destination " << dest_ << " not supported." << std::endl;
        throw eckit::UserError(ss.str(), Here());
    }

    if (store_.canMoveTo(catalogue_.key(), catalogue_.config(), dest_)) {
        move();
    }

    return false;  // Don't explore indexes, etc. We have already moved them away...
}

void TocMoveVisitor::move() {

    store_.moveTo(catalogue_.key(), catalogue_.config(), dest_, queue_);

    eckit::PathName destPath = dest_.path();
    for (const eckit::PathName& root : CatalogueRootManager(catalogue_.config()).canMoveToRoots(catalogue_.key())) {
        if (root.sameAs(destPath)) {
            eckit::PathName dest_db = destPath / catalogue_.basePath().baseName(true);

            if (!dest_db.exists()) {
                dest_db.mkdir();
            }

            DIR* dirp = ::opendir(catalogue_.basePath().c_str());
            struct dirent* dp;
            while ((dp = readdir(dirp)) != NULL) {
                if (strstr(dp->d_name, ".index") || strstr(dp->d_name, "toc.") || strstr(dp->d_name, "schema")) {

                    FileCopy fileCopy(catalogue_.basePath(), dest_db, dp->d_name);
                    queue_.emplace(fileCopy);
                }
            }
            closedir(dirp);

            FileCopy toc(catalogue_.basePath(), dest_db, "toc", true);
            queue_.emplace(toc);

            dirp = ::opendir(catalogue_.basePath().c_str());
            while ((dp = readdir(dirp)) != NULL) {
                if (strstr(dp->d_name, ".lock") || strstr(dp->d_name, "duplicates.allow")) {

                    FileCopy fileCopy(catalogue_.basePath(), dest_db, dp->d_name);
                    queue_.emplace(fileCopy);
                }
            }
            closedir(dirp);
            FileCopy folder(catalogue_.basePath(), "", "");
            queue_.emplace(folder);
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
