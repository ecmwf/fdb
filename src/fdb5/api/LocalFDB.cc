/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
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
#include "fdb5/LibFdb.h"

#include "fdb5/api/visitors/QueryVisitor.h"
#include "fdb5/api/visitors/ListVisitor.h"
#include "fdb5/api/visitors/DumpVisitor.h"
#include "fdb5/api/visitors/WhereVisitor.h"
#include "fdb5/api/visitors/WipeVisitor.h"
#include "fdb5/api/visitors/PurgeVisitor.h"
#include "fdb5/api/visitors/StatsVisitor.h"

#include "marslib/MarsTask.h"

using namespace fdb5::api::visitor;


namespace fdb5 {

static FDBBuilder<LocalFDB> localFdbBuilder("local");


namespace {

//----------------------------------------------------------------------------------------------------------------------

} // namespace


void LocalFDB::archive(const Key& key, const void* data, size_t length) {

    if (!archiver_) {
        eckit::Log::debug<LibFdb>() << *this << ": Constructing new archiver" << std::endl;
        archiver_.reset(new Archiver(config_));
    }

    archiver_->archive(key, data, length);
}


eckit::DataHandle *LocalFDB::retrieve(const MarsRequest &request) {

    if (!retriever_) {
        eckit::Log::debug<LibFdb>() << *this << ": Constructing new retriever" << std::endl;
        retriever_.reset(new Retriever(config_));
    }

    MarsRequest e("environ");
    MarsTask task(request, e);

    return retriever_->retrieve(task);
}

template<typename VisitorType, typename ... Ts>
APIIterator<typename VisitorType::ValueType> LocalFDB::queryInternal(const FDBToolRequest& request, Ts ... args) {

    using ValueType = typename VisitorType::ValueType;
    using QueryIterator = APIIterator<ValueType>;
    using AsyncIterator = APIAsyncIterator<ValueType>;


    auto async_worker = [this, request, args...] (eckit::Queue<ValueType>& queue) {
        EntryVisitMechanism mechanism(config_);
        VisitorType visitor(queue, args...);
        mechanism.visit(request, visitor);
    };

    return QueryIterator(new AsyncIterator(async_worker));
}


ListIterator LocalFDB::list(const FDBToolRequest& request) {
    Log::debug<LibFdb>() << "LocalFDB::list() : " << request << std::endl;
    return queryInternal<ListVisitor>(request);
}

DumpIterator LocalFDB::dump(const FDBToolRequest &request, bool simple) {
    Log::debug<LibFdb>() << "LocalFDB::dump() : " << request << std::endl;
    return queryInternal<DumpVisitor>(request, simple);
}

WhereIterator LocalFDB::where(const FDBToolRequest &request) {
    Log::debug<LibFdb>() << "LocalFDB::where() : " << request << std::endl;
    return queryInternal<WhereVisitor>(request);
}

WipeIterator LocalFDB::wipe(const FDBToolRequest &request, bool doit) {
    Log::debug<LibFdb>() << "LocalFDB::wipe() : " << request << std::endl;
    return queryInternal<WipeVisitor>(request, doit);
}

PurgeIterator LocalFDB::purge(const FDBToolRequest &request, bool doit) {
    Log::debug<LibFdb>() << "LocalFDB::purge() : " << request << std::endl;
    return queryInternal<PurgeVisitor>(request, doit);
}

StatsIterator LocalFDB::stats(const FDBToolRequest &request) {
    Log::debug<LibFdb>() << "LocalFDB::stats() : " << request << std::endl;
    return queryInternal<StatsVisitor>(request);

}

std::string LocalFDB::id() const {
    return config_.expandPath("~fdb");
}


void LocalFDB::flush() {
    if (archiver_) {
        archiver_->flush();
    }
}


void LocalFDB::print(std::ostream &s) const {
    s << "LocalFDB(home=" << config_.expandPath("~fdb") << ")";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
