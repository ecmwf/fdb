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

#include "fdb5/api/SelectFDB.h"
#include "eckit/log/Log.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/database/Key.h"
#include "fdb5/rules/SelectMatcher.h"

using namespace eckit;
using namespace metkit::mars;

namespace fdb5 {

static FDBBuilder<SelectFDB> selectFdbBuilder("select");

//----------------------------------------------------------------------------------------------------------------------

SelectFDB::FDBLane::FDBLane(const eckit::LocalConfiguration& config) :
    matcher_{config}, config_(config), fdb_(nullptr) {}

template <typename T>  // T is either a MarsRequest or Key
bool SelectFDB::FDBLane::matches(const T& vals, Matcher::MatchMissingPolicy matchOnMissing) const {
    return matcher_.match(vals, matchOnMissing);
}

FDBBase& SelectFDB::FDBLane::get() {
    if (!fdb_) {
        fdb_ = FDBFactory::instance().build(config_);
    }
    return *fdb_;
}

void SelectFDB::FDBLane::flush() {
    if (fdb_) {
        fdb_->flush();
    }
}

//----------------------------------------------------------------------------------------------------------------------

SelectFDB::SelectFDB(const Config& config, const std::string& name) : FDBBase(config, name) {

    ASSERT(config.getString("type", "") == "select");

    if (!config.has("fdbs")) {
        throw eckit::UserError("fdbs not specified for select FDB", Here());
    }

    std::string schema = config.getString("schema", "");
    for (auto& c : config.getSubConfigs("fdbs")) {
        /// inject parent schema into the SelectFDB sub-fdbs
        /// note: a sub-fdb can be a remote or select FDB, se we do not worry if neither parent nor children are
        /// defining a schema: it could be defined in the grand-children
        if (!schema.empty() && !c.has("schema")) {
            c.set("schema", schema);
        }
        subFdbs_.emplace_back(FDBLane{c});
    }
}


SelectFDB::~SelectFDB() {}

void SelectFDB::archive(const Key& key, const void* data, size_t length) {

    for (auto& lane : subFdbs_) {
        if (lane.matches(key, Matcher::DontMatchOnMissing)) {
            lane.get().archive(key, data, length);
            return;
        }
    }

    std::stringstream ss;
    ss << "No matching fdb for key: " << key;
    throw eckit::UserError(ss.str(), Here());
}

ListIterator SelectFDB::inspect(const MarsRequest& request) {

    std::queue<APIIterator<ListElement>> lists;

    for (auto& lane : subFdbs_) {
        if (lane.matches(request, Matcher::DontMatchOnMissing)) {
            lists.push(lane.get().inspect(request));
        }
    }

    return ListIterator(new ListAggregateIterator(std::move(lists)));
}

template <typename QueryFN>
auto SelectFDB::queryInternal(const FDBToolRequest& request, const QueryFN& fn)
    -> decltype(fn(*(FDBBase*)(nullptr), request)) {

    using QueryIterator = decltype(fn(*(FDBBase*)(nullptr), request));
    using ValueType = typename QueryIterator::value_type;

    std::queue<APIIterator<ValueType>> iterQueue;

    for (auto& lane : subFdbs_) {
        if (request.all() || lane.matches(request.request(), Matcher::MatchOnMissing)) {
            iterQueue.push(fn(lane.get(), request));
        }
    }

    return QueryIterator(new APIAggregateIterator<ValueType>(std::move(iterQueue)));
}

ListIterator SelectFDB::list(const FDBToolRequest& request, const int level) {
    LOG_DEBUG_LIB(LibFdb5) << "SelectFDB::list() >> " << request << std::endl;
    return queryInternal(request,
                         [level](FDBBase& fdb, const FDBToolRequest& request) { return fdb.list(request, level); });
}

DumpIterator SelectFDB::dump(const FDBToolRequest& request, bool simple) {
    LOG_DEBUG_LIB(LibFdb5) << "SelectFDB::dump() >> " << request << std::endl;
    return queryInternal(request,
                         [simple](FDBBase& fdb, const FDBToolRequest& request) { return fdb.dump(request, simple); });
}

StatusIterator SelectFDB::status(const FDBToolRequest& request) {
    LOG_DEBUG_LIB(LibFdb5) << "SelectFDB::status() >> " << request << std::endl;
    return queryInternal(request, [](FDBBase& fdb, const FDBToolRequest& request) { return fdb.status(request); });
}

WipeIterator SelectFDB::wipe(const FDBToolRequest& request, bool doit, bool porcelain, bool unsafeWipeAll) {
    LOG_DEBUG_LIB(LibFdb5) << "SelectFDB::wipe() >> " << request << std::endl;
    return queryInternal(request, [doit, porcelain, unsafeWipeAll](FDBBase& fdb, const FDBToolRequest& request) {
        return fdb.wipe(request, doit, porcelain, unsafeWipeAll);
    });
}

PurgeIterator SelectFDB::purge(const FDBToolRequest& request, bool doit, bool porcelain) {
    LOG_DEBUG_LIB(LibFdb5) << "SelectFDB::purge() >> " << request << std::endl;
    return queryInternal(request, [doit, porcelain](FDBBase& fdb, const FDBToolRequest& request) {
        return fdb.purge(request, doit, porcelain);
    });
}

StatsIterator SelectFDB::stats(const FDBToolRequest& request) {
    LOG_DEBUG_LIB(LibFdb5) << "SelectFDB::stats() >> " << request << std::endl;
    return queryInternal(request, [](FDBBase& fdb, const FDBToolRequest& request) { return fdb.stats(request); });
}

ControlIterator SelectFDB::control(const FDBToolRequest& request, ControlAction action,
                                   ControlIdentifiers identifiers) {
    LOG_DEBUG_LIB(LibFdb5) << "SelectFDB::control >> " << request << std::endl;
    return queryInternal(request, [action, identifiers](FDBBase& fdb, const FDBToolRequest& request) {
        return fdb.control(request, action, identifiers);
    });
}

AxesIterator SelectFDB::axesIterator(const FDBToolRequest& request, int level) {
    LOG_DEBUG_LIB(LibFdb5) << "SelectFDB::axesIterator() >> " << request << std::endl;
    return queryInternal(
        request, [level](FDBBase& fdb, const FDBToolRequest& request) { return fdb.axesIterator(request, level); });
}

void SelectFDB::flush() {
    for (auto& lane : subFdbs_) {
        lane.flush();
    }
}


void SelectFDB::print(std::ostream& s) const {
    s << "SelectFDB()";
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
