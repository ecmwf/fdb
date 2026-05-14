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
#include <sstream>
#include <vector>
#include "eckit/log/CodeLocation.h"
#include "eckit/log/Log.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/FDBFactory.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/WipeState.h"
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

void SelectFDB::FDBLane::flush(const std::string& tracingID) {
    if (fdb_) {
        fdb_->flush(tracingID);
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

void SelectFDB::archive(const Key& key, const void* data, size_t length, const std::string& tracingID) {

    for (auto& lane : subFdbs_) {
        if (lane.matches(key, Matcher::DontMatchOnMissing)) {
            lane.get().archive(key, data, length, tracingID);
            return;
        }
    }

    std::ostringstream ss;
    ss << "tracingID: " << tracingID << " - No matching fdb for key: " << key;
    throw eckit::UserError(ss.str(), Here());
}

ListIterator SelectFDB::inspect(const MarsRequest& request, const std::string& tracingID) {

    std::queue<APIIterator<ListElement>> lists;

    for (auto& lane : subFdbs_) {
        if (lane.matches(request, Matcher::DontMatchOnMissing)) {
            lists.push(lane.get().inspect(request, tracingID));
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

ListIterator SelectFDB::list(const FDBToolRequest& request, const std::string& tracingID, const int level) {
    LOG_DEBUG_LIB(LibFdb5) << "tracingID: " << tracingID << " - SelectFDB::list() >> " << request << std::endl;
    return queryInternal(request, [tracingID, level](FDBBase& fdb, const FDBToolRequest& request) {
        return fdb.list(request, tracingID, level);
    });
}

DumpIterator SelectFDB::dump(const FDBToolRequest& request, const std::string& tracingID, bool simple) {
    LOG_DEBUG_LIB(LibFdb5) << "tracingID: " << tracingID << " - SelectFDB::dump() >> " << request << std::endl;
    return queryInternal(request, [tracingID, simple](FDBBase& fdb, const FDBToolRequest& request) {
        return fdb.dump(request, tracingID, simple);
    });
}

StatusIterator SelectFDB::status(const FDBToolRequest& request, const std::string& tracingID) {
    LOG_DEBUG_LIB(LibFdb5) << "tracingID: " << tracingID << " - SelectFDB::status() >> " << request << std::endl;
    return queryInternal(
        request, [tracingID](FDBBase& fdb, const FDBToolRequest& request) { return fdb.status(request, tracingID); });
}

WipeStateIterator SelectFDB::wipe(const FDBToolRequest& request, const std::string& tracingID, bool doit,
                                  bool porcelain, bool unsafeWipeAll) {
    LOG_DEBUG_LIB(LibFdb5) << "tracingID: " << tracingID << " - SelectFDB::wipe() >> " << request << std::endl;

    FDBLane* matchingLane = nullptr;
    for (auto& lane : subFdbs_) {
        if (lane.matches(request.request(), Matcher::MatchOnMissing)) {
            if (matchingLane != nullptr) {
                std::stringstream ss;
                ss << "tracingID: " << tracingID << " - Multiple matching lanes for request " << request.request();
                ss << " - wipe request must not match multiple SelectFDB lanes.";
                throw eckit::UserError(ss.str(), Here());
            }

            matchingLane = &lane;
        }
    }

    if (matchingLane == nullptr) {
        std::stringstream ss;
        ss << "tracingID: " << tracingID << " - No matching lane for request " << request.request();
        throw eckit::UserError(ss.str(), Here());
    }

    return matchingLane->get().wipe(request, tracingID, doit, porcelain, unsafeWipeAll);
}

PurgeIterator SelectFDB::purge(const FDBToolRequest& request, const std::string& tracingID, bool doit, bool porcelain) {
    LOG_DEBUG_LIB(LibFdb5) << "tracingID: " << tracingID << " - SelectFDB::purge() >> " << request << std::endl;
    return queryInternal(request, [tracingID, doit, porcelain](FDBBase& fdb, const FDBToolRequest& request) {
        return fdb.purge(request, tracingID, doit, porcelain);
    });
}

StatsIterator SelectFDB::stats(const FDBToolRequest& request, const std::string& tracingID) {
    LOG_DEBUG_LIB(LibFdb5) << "tracingID: " << tracingID << " - SelectFDB::stats() >> " << request << std::endl;
    return queryInternal(
        request, [tracingID](FDBBase& fdb, const FDBToolRequest& request) { return fdb.stats(request, tracingID); });
}

ControlIterator SelectFDB::control(const FDBToolRequest& request, const std::string& tracingID, ControlAction action,
                                   ControlIdentifiers identifiers) {
    LOG_DEBUG_LIB(LibFdb5) << "tracingID: " << tracingID << " - SelectFDB::control >> " << request << std::endl;
    return queryInternal(request, [tracingID, action, identifiers](FDBBase& fdb, const FDBToolRequest& request) {
        return fdb.control(request, tracingID, action, identifiers);
    });
}

AxesIterator SelectFDB::axesIterator(const FDBToolRequest& request, const std::string& tracingID, int level) {
    LOG_DEBUG_LIB(LibFdb5) << "tracingID: " << tracingID << " - SelectFDB::axesIterator() >> " << request << std::endl;
    return queryInternal(request, [tracingID, level](FDBBase& fdb, const FDBToolRequest& request) {
        return fdb.axesIterator(request, tracingID, level);
    });
}

void SelectFDB::flush(const std::string& tracingID) {
    for (auto& lane : subFdbs_) {
        lane.flush(tracingID);
    }
}


void SelectFDB::print(std::ostream& s) const {
    s << "SelectFDB()";
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
