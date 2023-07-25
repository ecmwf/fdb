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
#include "fdb5/database/DB.h"
#include "fdb5/toc/TocCatalogue.h"
#include "fdb5/toc/TocMoveVisitor.h"
#include "fdb5/toc/RootManager.h"

#include <dirent.h>
#include <errno.h>
#include <fdb5/LibFdb5.h>
#include <sys/types.h>
#include <sys/file.h>
#include <cstring>


using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

// TODO: Warnings and errors form inside here back to the user.

TocMoveVisitor::TocMoveVisitor(const TocCatalogue& catalogue,
                               const Store& store,
                               const metkit::mars::MarsRequest& request,
                               const eckit::URI& dest,
                               bool removeSrc,
                               int removeDelay,
                               eckit::Transport& transport) :
    MoveVisitor(request, dest, removeSrc, removeDelay, transport),
    catalogue_(catalogue),
    store_(store) {}

TocMoveVisitor::~TocMoveVisitor() {}

bool TocMoveVisitor::visitDatabase(const Catalogue& catalogue, const Store& store) {

    // Overall checks
    ASSERT(&catalogue_ == &catalogue);

    MoveVisitor::visitDatabase(catalogue_, store);

    // TOC specific checks: index files not locked
    DIR* dirp = ::opendir(catalogue_.basePath().asString().c_str());
    struct dirent* dp;
    while ((dp = readdir(dirp)) != NULL) {
        if (strstr( dp->d_name, ".index")) {
            eckit::PathName src_ = catalogue_.basePath() / dp->d_name;
            int fd = ::open(src_.asString().c_str(), O_RDWR);
            if(::flock(fd, LOCK_EX)) {
                std::stringstream ss;
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
        
        for (const eckit::PathName& root: CatalogueRootManager(catalogue_.config()).canMoveToRoots(catalogue_.key())) {

            if (root.sameAs(destPath)) {
                eckit::PathName dest_db = destPath / catalogue_.basePath().baseName(true);

                if(dest_db.exists()) {
                    std::stringstream ss;
                    ss << "Target folder already exist!" << std::endl;
                    throw UserError(ss.str(), Here());
                }
                found = true;
                break;
            }
        }
        if (!found) {
            std::stringstream ss;
            ss << "Destination " << dest_ << " cannot be used to archive a DB with key: " << catalogue_.key() << std::endl;
            throw eckit::UserError(ss.str(), Here());
        }
    } else {
        std::stringstream ss;
        ss << "Destination " << dest_ << " not supported." << std::endl;
        throw eckit::UserError(ss.str(), Here());
    }

    if (store_.canMoveTo(catalogue_.key(), catalogue_.config(), dest_)) {
        move();
    }

    return false; // Don't explore indexes, etc. We have already moved them away...
}

void TocMoveVisitor::move() {

    // int numThreads = eckit::Resource<int>("fdbMoveThreads;$FDB_MOVE_THREADS", threads_);
    // bool mpi = eckit::Resource<bool>("fdbMoveMpi;$FDB_MOVE_MPI", mpi_);

    store_.moveTo(catalogue_.key(), catalogue_.config(), dest_, transport_);

    eckit::PathName destPath = dest_.path();
    for (const eckit::PathName& root: CatalogueRootManager(catalogue_.config()).canMoveToRoots(catalogue_.key())) {
        if (root.sameAs(destPath)) {
            eckit::PathName dest_db = destPath / catalogue_.basePath().baseName(true);

            if(!dest_db.exists()) {
                dest_db.mkdir();
            }
            
//            eckit::ThreadPool pool("catalogue"+catalogue_.basePath().asString(), numThreads);

            DIR* dirp = ::opendir(catalogue_.basePath().asString().c_str());
            struct dirent* dp;
            while ((dp = readdir(dirp)) != NULL) {
                if (strstr( dp->d_name, ".index") ||
                    strstr( dp->d_name, "toc.") ||
                    strstr( dp->d_name, "schema")) {

                    eckit::Message message;
                    FileCopy fileCopy(catalogue_.basePath(), dest_db, dp->d_name);
                    fileCopy.encode(message);
                    transport_.sendMessageToNextWorker(message);
//                pool.push(it->second);
                }
            }
            closedir(dirp);

            transport_.synchronise();

            FileCopy(catalogue_.basePath(), dest_db, "toc").execute();

            if (removeSrc_) {
                sleep(removeDelay_);

                eckit::PathName catalogueFile = catalogue_.basePath() / "toc";
                eckit::Log::debug<LibFdb5>() << "Removing " << catalogueFile << std::endl;
                catalogueFile.unlink(false);

                dirp = ::opendir(catalogue_.basePath().asString().c_str());
                while ((dp = readdir(dirp)) != NULL) {
                    if (strstr( dp->d_name, ".index") ||
                        strstr( dp->d_name, "toc.") ||
                        strstr( dp->d_name, "schema") ||
                        strstr( dp->d_name, ".lock") ||
                        strstr( dp->d_name, "duplicates.allow")) {

                        catalogueFile = catalogue_.basePath() / dp->d_name;
                        eckit::Log::debug<LibFdb5>() << "Removing " << catalogueFile << std::endl;
                        catalogueFile.unlink(false);
                    }
                }
                closedir(dirp);

                store_.remove(catalogue_.key());
                
                eckit::Log::debug<LibFdb5>() << "Removing " << catalogue_.basePath() << std::endl;
                catalogue_.basePath().rmdir(false);
            }
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
