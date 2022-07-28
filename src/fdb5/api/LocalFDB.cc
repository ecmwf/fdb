/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#include <dirent.h>
#include <fcntl.h>

#include "eckit/container/Queue.h"
#include "eckit/io/FileHandle.h"
#include "eckit/log/Log.h"
#include "eckit/message/Message.h"
#include "eckit/thread/ThreadPool.h"


#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/LocalFDB.h"
#include "fdb5/database/Archiver.h"
#include "fdb5/database/DB.h"
#include "fdb5/database/EntryVisitMechanism.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Inspector.h"
#include "fdb5/database/Key.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/LibFdb5.h"

#include "fdb5/api/local/ControlVisitor.h"
#include "fdb5/api/local/DumpVisitor.h"
#include "fdb5/api/local/ListVisitor.h"
#include "fdb5/api/local/PurgeVisitor.h"
#include "fdb5/api/local/QueryVisitor.h"
#include "fdb5/api/local/StatsVisitor.h"
#include "fdb5/api/local/StatusVisitor.h"
#include "fdb5/api/local/WipeVisitor.h"


using namespace fdb5::api::local;
using namespace eckit;


namespace fdb5 {
void LocalFDB::archive(const Key& key, const void* data, size_t length) {

    if (!archiver_) {
        Log::debug<LibFdb5>() << *this << ": Constructing new archiver" << std::endl;
        archiver_.reset(new Archiver(config_));
    }

    archiver_->archive(key, data, length);
}

ListIterator LocalFDB::inspect(const metkit::mars::MarsRequest &request) {

    if (!inspector_) {
        Log::debug<LibFdb5>() << *this << ": Constructing new retriever" << std::endl;
        inspector_.reset(new Inspector(config_));
    }

    return inspector_->inspect(request);
}

template<typename VisitorType, typename ... Ts>
APIIterator<typename VisitorType::ValueType> LocalFDB::queryInternal(const FDBToolRequest& request, Ts ... args) {

    using ValueType = typename VisitorType::ValueType;
    using QueryIterator = APIIterator<ValueType>;
    using AsyncIterator = APIAsyncIterator<ValueType>;

    auto async_worker = [this, request, args...] (Queue<ValueType>& queue) {
        EntryVisitMechanism mechanism(config_);
        VisitorType visitor(queue, request.request(), args...);
        mechanism.visit(request, visitor);
    };

    return QueryIterator(new AsyncIterator(async_worker));
}

ListIterator LocalFDB::list(const FDBToolRequest& request) {
    Log::debug<LibFdb5>() << "LocalFDB::list() : " << request << std::endl;
    return queryInternal<ListVisitor>(request);
}

DumpIterator LocalFDB::dump(const FDBToolRequest &request, bool simple) {
    Log::debug<LibFdb5>() << "LocalFDB::dump() : " << request << std::endl;
    return queryInternal<DumpVisitor>(request, simple);
}

StatusIterator LocalFDB::status(const FDBToolRequest &request) {
    Log::debug<LibFdb5>() << "LocalFDB::status() : " << request << std::endl;
    return queryInternal<StatusVisitor>(request);
}

WipeIterator LocalFDB::wipe(const FDBToolRequest &request, bool doit, bool porcelain, bool unsafeWipeAll) {
    Log::debug<LibFdb5>() << "LocalFDB::wipe() : " << request << std::endl;
    return queryInternal<fdb5::api::local::WipeVisitor>(request, doit, porcelain, unsafeWipeAll);
}

PurgeIterator LocalFDB::purge(const FDBToolRequest& request, bool doit, bool porcelain) {
    Log::debug<LibFdb5>() << "LocalFDB::purge() : " << request << std::endl;
    return queryInternal<fdb5::api::local::PurgeVisitor>(request, doit, porcelain);
}

StatsIterator LocalFDB::stats(const FDBToolRequest& request) {
    Log::debug<LibFdb5>() << "LocalFDB::stats() : " << request << std::endl;
    return queryInternal<StatsVisitor>(request);
}

ControlIterator LocalFDB::control(const FDBToolRequest& request,
                                  ControlAction action,
                                  ControlIdentifiers identifiers) {
    Log::debug<LibFdb5>() << "LocalFDB::control() : " << request << std::endl;
    return queryInternal<ControlVisitor>(request, action, identifiers);
}

bool LocalFDB::canMove(const FDBToolRequest& request) {

    const Schema& schema = config_.schema();

    Key source;
    ASSERT(schema.expandFirstLevel(request.request(), source));

    std::unique_ptr<DB> dbSource = DB::buildReader(source, config_);
    if (!dbSource->exists()) {
        std::stringstream ss;
        ss << "Source database not found: " << source << std::endl;
        throw UserError(ss.str(), Here());
    }

    return dbSource->canMove();
}

// class for writing a chunk of the user buffer - used to perform multiple simultaneous writes
class FileCopy : public eckit::ThreadPoolTask {
    eckit::PathName src_;
    eckit::PathName dest_;

    void execute() {
        eckit::FileHandle src(src_);
        eckit::FileHandle dest(dest_);
        src.copyTo(dest);
    }

public:
    FileCopy(const eckit::PathName& srcPath, const eckit::PathName& destPath, const std::string& fileName):
        src_(srcPath / fileName), dest_(destPath / fileName) {}
};

void LocalFDB::move(const ControlElement& elem, const eckit::PathName& dest) {

    eckit::PathName db = elem.location.path().baseName(true);

    eckit::PathName dest_db = dest / db;

    eckit::URI destURI("toc", dest_db);

    if(dest_db.exists()) {
        std::stringstream ss;
        ss << "Target folder already exist!" << std::endl;
        throw UserError(ss.str(), Here());
    }

    dest_db.mkdir();

    Log::info() << "Database: " << elem.key << std::endl
                << "  location: " << elem.location.asString() << std::endl
                << "  new location: " << dest_db << std::endl;

    eckit::ThreadPool pool(elem.location.asString(), 4);

    DIR* dirp = ::opendir(elem.location.asString().c_str());
    struct dirent* dp;
    while ((dp = readdir(dirp)) != NULL) {
        if (strstr( dp->d_name, ".index") ||
            strstr( dp->d_name, ".data") ||
            strstr( dp->d_name, "toc.") ||
            strstr( dp->d_name, "schema")) {

            pool.push(new FileCopy(elem.location.path(), dest_db, dp->d_name));
        }
    }
    closedir(dirp);

    pool.wait();
    pool.push(new FileCopy(elem.location.path(), dest_db, "toc"));
    pool.wait();
}

void LocalFDB::flush() {
    if (archiver_) {
        archiver_->flush();
    }
}


void LocalFDB::print(std::ostream &s) const {
    s << "LocalFDB(home=" << config_.expandPath("~fdb") << ")";
}


static FDBBuilder<LocalFDB> localFdbBuilder("local");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
