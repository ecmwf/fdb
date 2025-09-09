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
#include "eckit/message/Message.h"
#include "eckit/types/Types.h"
#include "eckit/utils/Tokenizer.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/SelectFDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/io/HandleGatherer.h"

using namespace eckit;
using namespace metkit::mars;
namespace fdb5 {

static FDBBuilder<SelectFDB> selectFdbBuilder("select");

//----------------------------------------------------------------------------------------------------------------------

namespace {  // helpers

using ValuesMap = std::map<std::string, std::vector<std::string>>;

void insertIfMissing(ValuesMap& out, const std::string& keyword, const Key& key) {
    if (out.find(keyword) != out.end()) {
        return;
    }
    const auto [it, found] = key.find(keyword);
    out[keyword]           = found ? std::vector<std::string>{it->second} : std::vector<std::string>{};
}

}  // namespace

SelectFDB::FDBLane::FDBLane(const eckit::LocalConfiguration& config) :
    select_{Matcher(config.getString("select", ""))}, config_(config), fdb_(std::nullopt) {

    if (config.has("filter")) {
        for (const auto& f : config.getSubConfigurations("filter")) {
            if (!f.has("exclude")) {
                continue;
            }
            excludes_.push_back(Matcher(f.getString("exclude", "")));
        }
    }
}

bool SelectFDB::FDBLane::matches(const Key& key, bool matchOnMissing) const {
    const auto vals = collectValues(key);
    return matchesValues(vals, matchOnMissing);
}

bool SelectFDB::FDBLane::matches(const MarsRequest& request, bool matchOnMissing) const {
    return matchesValues(request, matchOnMissing);
}

// collect (keys, values) in request to be checked against select and exclude conditions
ValuesMap SelectFDB::FDBLane::collectValues(const Key& key) const {
    ValuesMap out;
    for (const auto& [k, _] : select_.regexMap()) {
        insertIfMissing(out, k, key);
    }
    for (const auto& em : excludes_) {
        for (const auto& [k, _] : em.regexMap()) {
            insertIfMissing(out, k, key);
        }
    }
    return out;
}

template <typename T> // T is either a mars request or a map <string, vector<string>>
bool SelectFDB::FDBLane::matchesValues(const T& vals, bool matchOnMissing) const {

    if (!select_.match(vals, matchOnMissing, Matcher::ValuePolicy::Any))
        return false;

    bool excluded = std::any_of(excludes_.begin(), excludes_.end(),
                                [&](const auto& ex) { return ex.match(vals, false, Matcher::ValuePolicy::All); });

    return !excluded;
}

FDB& SelectFDB::FDBLane::get() {
    if (!fdb_) {
        fdb_.emplace(config_);
    }
    return *fdb_;
}

void SelectFDB::FDBLane::flush() {
    if (fdb_) {
        fdb_->flush();
    }
}

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
        if (lane.matches(key, false)) {
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
        // If we want to allow non-fully-specified retrieves, make false here.
        if (lane.matches(request, false)) {
            lists.push(lane.get().inspect(request));
        }
    }

    return ListIterator(new ListAggregateIterator(std::move(lists)));
}

template <typename QueryFN>
auto SelectFDB::queryInternal(const FDBToolRequest& request, const QueryFN& fn)
    -> decltype(fn(*(FDB*)(nullptr), request)) {

    using QueryIterator = decltype(fn(*(FDB*)(nullptr), request));
    using ValueType     = typename QueryIterator::value_type;

    std::queue<APIIterator<ValueType>> iterQueue;

    for (auto& lane : subFdbs_) {
        if (request.all() || lane.matches(request.request(), true)) {
            iterQueue.push(fn(lane.get(), request));
        }
    }

    return QueryIterator(new APIAggregateIterator<ValueType>(std::move(iterQueue)));
}

ListIterator SelectFDB::list(const FDBToolRequest& request, const int level) {
    LOG_DEBUG_LIB(LibFdb5) << "SelectFDB::list() >> " << request << std::endl;
    return queryInternal(request,
                         [level](FDB& fdb, const FDBToolRequest& request) { return fdb.list(request, false, level); });
}

DumpIterator SelectFDB::dump(const FDBToolRequest& request, bool simple) {
    LOG_DEBUG_LIB(LibFdb5) << "SelectFDB::dump() >> " << request << std::endl;
    return queryInternal(request,
                         [simple](FDB& fdb, const FDBToolRequest& request) { return fdb.dump(request, simple); });
}

StatusIterator SelectFDB::status(const FDBToolRequest& request) {
    LOG_DEBUG_LIB(LibFdb5) << "SelectFDB::status() >> " << request << std::endl;
    return queryInternal(request, [](FDB& fdb, const FDBToolRequest& request) { return fdb.status(request); });
}

WipeIterator SelectFDB::wipe(const FDBToolRequest& request, bool doit, bool porcelain, bool unsafeWipeAll) {
    LOG_DEBUG_LIB(LibFdb5) << "SelectFDB::wipe() >> " << request << std::endl;
    return queryInternal(request, [doit, porcelain, unsafeWipeAll](FDB& fdb, const FDBToolRequest& request) {
        return fdb.wipe(request, doit, porcelain, unsafeWipeAll);
    });
}

PurgeIterator SelectFDB::purge(const FDBToolRequest& request, bool doit, bool porcelain) {
    LOG_DEBUG_LIB(LibFdb5) << "SelectFDB::purge() >> " << request << std::endl;
    return queryInternal(request, [doit, porcelain](FDB& fdb, const FDBToolRequest& request) {
        return fdb.purge(request, doit, porcelain);
    });
}

StatsIterator SelectFDB::stats(const FDBToolRequest& request) {
    LOG_DEBUG_LIB(LibFdb5) << "SelectFDB::stats() >> " << request << std::endl;
    return queryInternal(request, [](FDB& fdb, const FDBToolRequest& request) { return fdb.stats(request); });
}

ControlIterator SelectFDB::control(const FDBToolRequest& request, ControlAction action,
                                   ControlIdentifiers identifiers) {
    LOG_DEBUG_LIB(LibFdb5) << "SelectFDB::control >> " << request << std::endl;
    return queryInternal(request, [action, identifiers](FDB& fdb, const FDBToolRequest& request) {
        return fdb.control(request, action, identifiers);
    });
}

AxesIterator SelectFDB::axesIterator(const FDBToolRequest& request, int level) {
    LOG_DEBUG_LIB(LibFdb5) << "SelectFDB::axesIterator() >> " << request << std::endl;
    return queryInternal(request,
                         [level](FDB& fdb, const FDBToolRequest& request) { return fdb.axesIterator(request, level); });
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
