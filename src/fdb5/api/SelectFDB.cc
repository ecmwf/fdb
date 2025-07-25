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

namespace fdb5 {

static FDBBuilder<SelectFDB> selectFdbBuilder("select");

//----------------------------------------------------------------------------------------------------------------------


std::map<std::string, eckit::Regex> parseFDBSelect(const eckit::LocalConfiguration& config) {

    std::map<std::string, eckit::Regex> selectDict;

    // Select operates as constraints of the form: class=regex,key=regex,...
    // By default, there is no select.

    std::string select = config.getString("select", "");

    std::vector<std::string> select_key_values;
    eckit::Tokenizer(',')(select, select_key_values);

    eckit::Tokenizer equalsTokenizer('=');
    for (const std::string& key_value : select_key_values) {
        std::vector<std::string> kv;
        equalsTokenizer(key_value, kv);

        if (kv.size() != 2 || selectDict.find(kv[0]) != selectDict.end()) {
            std::stringstream ss;
            ss << "Invalid select condition for pool: " << select << std::endl;
            throw eckit::UserError(ss.str(), Here());
        }

        selectDict[kv[0]] = eckit::Regex(kv[1]);
    }

    return selectDict;
}

SelectFDB::FDBLane::FDBLane(const eckit::LocalConfiguration& config) :
    select_(parseFDBSelect(config)), config_(config), fdb_(std::nullopt) {}


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
        if (matches(key, lane.select(), true)) {
            lane.get().archive(key, data, length);
            return;
        }
    }

    std::stringstream ss;
    ss << "No matching fdb for key: " << key;
    throw eckit::UserError(ss.str(), Here());
}

ListIterator SelectFDB::inspect(const metkit::mars::MarsRequest& request) {

    std::queue<APIIterator<ListElement>> lists;

    for (auto& lane : subFdbs_) {
        // If we want to allow non-fully-specified retrieves, make false here.
        if (matches(request, lane.select(), true)) {
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
        if (request.all() || matches(request.request(), lane.select(), false)) {
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

bool SelectFDB::matches(const Key& key, const SelectMap& select, bool requireMissing) const {

    for (const auto& [keyword, regex] : select) {
        const auto [iter, found] = key.find(keyword);

        if (!found) {
            if (requireMissing) {
                return false;
            }
        }
        else {
            if (!regex.match(iter->second)) {
                return false;
            }
        }
    }
    return true;
}

bool SelectFDB::matches(const metkit::mars::MarsRequest& request, const SelectMap& select, bool requireMissing) const {

    for (const auto& [keyword, regex] : select) {

        const std::vector<std::string>& request_values = request.values(keyword, /* emptyOK */ true);

        if (request_values.size() == 0) {
            if (requireMissing)
                return false;
        }
        else {
            bool re_match = false;
            for (const std::string& v : request_values) {
                if (regex.match(v)) {
                    re_match = true;
                    break;
                }
            }
            if (!re_match)
                return false;
        }
    }

    return true;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
