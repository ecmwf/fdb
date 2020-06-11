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

#include "eckit/log/Log.h"
#include "eckit/container/Queue.h"

#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/LocalFDB.h"
#include "fdb5/database/Archiver.h"
#include "fdb5/database/EntryVisitMechanism.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Retriever.h"
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

DataHandle *LocalFDB::retrieve(const metkit::mars::MarsRequest &request) {

    if (!retriever_) {
        Log::debug<LibFdb5>() << *this << ": Constructing new retriever" << std::endl;
        retriever_.reset(new Retriever(config_));
    }

    return retriever_->retrieve(request);
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
