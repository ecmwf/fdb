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

#include "fdb5/api/LocalFDB.h"

#include "eckit/container/Queue.h"
#include "eckit/log/Log.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/api/local/AxesVisitor.h"
#include "fdb5/api/local/ControlVisitor.h"
#include "fdb5/api/local/DumpVisitor.h"
#include "fdb5/api/local/ListVisitor.h"
#include "fdb5/api/local/MoveVisitor.h"
#include "fdb5/api/local/PurgeVisitor.h"
#include "fdb5/api/local/StatsVisitor.h"
#include "fdb5/api/local/StatusVisitor.h"
#include "fdb5/api/local/WipeVisitor.h"
#include "fdb5/database/Archiver.h"
#include "fdb5/database/EntryVisitMechanism.h"
#include "fdb5/database/Inspector.h"
#include "fdb5/database/Key.h"
#include "fdb5/rules/Schema.h"


using namespace fdb5::api::local;
using namespace eckit;


namespace fdb5 {

void LocalFDB::archive(const Key& key, const void* data, size_t length) {

    if (!archiver_) {
        LOG_DEBUG_LIB(LibFdb5) << *this << ": Constructing new archiver" << std::endl;
        archiver_.reset(new Archiver(config_, archiveCallback_));
    }

    archiver_->archive(key, data, length);
}

void LocalFDB::reindex(const Key& key, const FieldLocation& location) {
    if (!reindexer_) {
        LOG_DEBUG_LIB(LibFdb5) << *this << ": Constructing new reindexer" << std::endl;
        reindexer_.reset(new Reindexer(config_));
    }

    reindexer_->reindex(key, location);
}

ListIterator LocalFDB::inspect(const metkit::mars::MarsRequest& request) {

    if (!inspector_) {
        LOG_DEBUG_LIB(LibFdb5) << *this << ": Constructing new retriever" << std::endl;
        inspector_.reset(new Inspector(config_));
    }

    return inspector_->inspect(request);
}

template <typename VisitorType, typename... Ts>
APIIterator<typename VisitorType::ValueType> LocalFDB::queryInternal(const FDBToolRequest& request, Ts... args) {

    using ValueType     = typename VisitorType::ValueType;
    using QueryIterator = APIIterator<ValueType>;
    using AsyncIterator = FDBAsyncIterator<ValueType>;

    auto async_worker = [this, request, args...](Queue<ValueType>& queue) {
        EntryVisitMechanism mechanism(config_);
        VisitorType visitor(queue, request.request(), args...);
        mechanism.visit(request, visitor);
    };

    return QueryIterator(new AsyncIterator(shared_from_this(), async_worker));
}

ListIterator LocalFDB::list(const FDBToolRequest& request, const int level) {
    LOG_DEBUG_LIB(LibFdb5) << "LocalFDB::list() : " << request << std::endl;
    return queryInternal<ListVisitor>(request, level);
}

DumpIterator LocalFDB::dump(const FDBToolRequest& request, bool simple) {
    LOG_DEBUG_LIB(LibFdb5) << "LocalFDB::dump() : " << request << std::endl;
    return queryInternal<DumpVisitor>(request, simple);
}

StatusIterator LocalFDB::status(const FDBToolRequest& request) {
    LOG_DEBUG_LIB(LibFdb5) << "LocalFDB::status() : " << request << std::endl;
    return queryInternal<StatusVisitor>(request);
}

WipeIterator LocalFDB::wipe(const FDBToolRequest& request, bool doit, bool porcelain, bool unsafeWipeAll) {
    LOG_DEBUG_LIB(LibFdb5) << "LocalFDB::wipe() : " << request << std::endl;
    return queryInternal<fdb5::api::local::WipeVisitor>(request, doit, porcelain, unsafeWipeAll);
}

MoveIterator LocalFDB::move(const FDBToolRequest& request, const eckit::URI& dest) {
    LOG_DEBUG_LIB(LibFdb5) << "LocalFDB::move() : " << request << std::endl;
    return queryInternal<fdb5::api::local::MoveVisitor>(request, dest);
}

PurgeIterator LocalFDB::purge(const FDBToolRequest& request, bool doit, bool porcelain) {
    LOG_DEBUG_LIB(LibFdb5) << "LocalFDB::purge() : " << request << std::endl;
    return queryInternal<fdb5::api::local::PurgeVisitor>(request, doit, porcelain);
}

StatsIterator LocalFDB::stats(const FDBToolRequest& request) {
    LOG_DEBUG_LIB(LibFdb5) << "LocalFDB::stats() : " << request << std::endl;
    return queryInternal<StatsVisitor>(request);
}

ControlIterator LocalFDB::control(const FDBToolRequest& request, ControlAction action, ControlIdentifiers identifiers) {
    LOG_DEBUG_LIB(LibFdb5) << "LocalFDB::control() : " << request << std::endl;
    return queryInternal<ControlVisitor>(request, action, identifiers);
}

AxesIterator LocalFDB::axesIterator(const FDBToolRequest& request, int level) {
    LOG_DEBUG_LIB(LibFdb5) << "LocalFDB::axesIterator() : " << request << std::endl;
    return queryInternal<AxesVisitor>(request, level);
}

void LocalFDB::flush() {
    ASSERT(!(archiver_ && reindexer_));
    if (archiver_) {
        archiver_->flush();
        flushCallback_();
    }
    else if (reindexer_) {
        reindexer_->flush();
        flushCallback_();
    }
}


void LocalFDB::print(std::ostream& s) const {
    s << "LocalFDB(home=" << config_.expandPath("~fdb") << ")";
}


static FDBBuilder<LocalFDB> localFdbBuilder("local");
static FDBBuilder<LocalFDB> builder("catalogue");  // Enable type=catalogue to build localFDB (serverside).
//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
