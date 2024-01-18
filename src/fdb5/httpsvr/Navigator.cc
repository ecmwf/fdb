/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/httpsvr/Navigator.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/rules/Schema.h"

#include "metkit/mars/MarsExpension.h"
#include "metkit/mars/MarsRequest.h"
#include "metkit/mars/TypeAny.h"

#include "eckit/utils/StringTools.h"
#include "eckit/web/Html.h"
#include "eckit/web/Url.h"
#include "eckit/log/JSON.h"

#include <ostream>
#include <unordered_map>

using namespace eckit;

namespace fdb5 {
namespace httpsvr {

//----------------------------------------------------------------------------------------------------------------------

namespace {

const std::string BASE_URL("/fdbnavigate");
const std::string TREE_BASE_URL("/fdbtree");

using lookup_t = std::unordered_map<Key, std::unordered_map<Key, std::set<Key>>>;

template <typename... Keys,
          std::enable_if_t<(std::is_convertible_v<const Key&, Keys>&& ...), int> = 0>
std::string navigate_url(bool json, const Keys&... keys) {
    std::string ret = BASE_URL + "?";
    for (const auto& key : {keys...}) {
        for (const auto& kv: key) {
            ret += kv.first + "=" + kv.second + "&";
        }
    }
    ret += "format=";
    ret += (json ? "json" : "html");
    return ret;
}

template <typename... Keys,
        std::enable_if_t<(std::is_convertible_v<const Key&, Keys>&& ...), int> = 0>
std::string tree_url(bool json, const Keys&... keys) {
    std::string ret = TREE_BASE_URL + "?";
    for (const auto& key : {keys...}) {
        for (const auto& kv: key) {
            ret += kv.first + "=" + kv.second + "&";
        }
    }
    ret += "format=";
    ret += (json ? "json" : "html");
    return ret;
}

void outputJSON(JSON& json, const lookup_t& lookup, int listLevel) {
    json << "databases";
    json.startList();
    for (const auto& db_kv : lookup) {
        json.startObject();
        json << "key" << db_kv.first;
        json << "navigate" << navigate_url(true, db_kv.first);
        json << "tree" << tree_url(true, db_kv.first);

        if (listLevel >= 1) {
            json << "indexes";
            json.startList();
            for (const auto& idx_kv : db_kv.second) {
                json.startObject();
                json << "key" << idx_kv.first;
                json << "navigate" << navigate_url(true, db_kv.first, idx_kv.first);
                json << "tree" << tree_url(true, db_kv.first, idx_kv.first);

                if (listLevel >= 2) {
                    json << "fields";
                    json.startList();
                    for (const auto& field : idx_kv.second) {
                        json.startObject();
                        json << "key" << field;
                        json << "navigate" << navigate_url(true, db_kv.first, idx_kv.first, field);
                        json << "tree" << tree_url(true, db_kv.first, idx_kv.first, field);
                        json.endObject();
                    }
                    json.endList();
                }
                json.endObject();
            }
            json.endList();
        }
        json.endObject();
    }
    json.endList();
}


const Key& keyAdapter(const Key& elem) {
    return elem;
}

template <typename T>
const Key& keyAdapter(const T& elem) {
    return elem.first;
}


template <typename Iterable,
          typename... Keys,
          std::enable_if_t<(std::is_convertible_v<const Key&, Keys>&& ...), int> = 0>
void outputTableKeys(const std::string& title, std::ostream& out, const Iterable& iterable,
                     const Keys&... parent_keys) {

    out << Html::BeginH1() << title << Html::EndH1();
    out << Html::BeginTable(true);

    std::map<std::string, int> columnMap;
    std::vector<std::string> columns;
    for (const auto& elem : iterable) {
        const auto& key = keyAdapter(elem);
        for (const auto& k : key.names()) {
            if (columnMap.find(k) == columnMap.end()) {
                columnMap.emplace(k, columnMap.size());
                columns.push_back(k);
            }
        }
    }

    // Header row
    out << Html::BeginRow();
    for (const auto& col : columns) {
        out << Html::BeginHeader() << col << Html::EndHeader();
    }
    out << Html::BeginHeader() << "Navigate" << Html::EndHeader();
    out << Html::BeginHeader() << "Tree" << Html::EndHeader();
    out << Html::EndRow();

    // n.b. although the keys in a Key have a well defined order, some may
    //      be missing, due to optional values. Therefore work from the master list
    for (const auto& elem : iterable) {
        const auto& key = keyAdapter(elem);
        out << Html::BeginRow();
        for (const auto& col : columns) {
            out << Html::BeginData();
            auto it = key.find(col);
            if (it != key.end()) {
                out << it->second;
            }
            out << Html::EndData();
        }
        out << Html::BeginData()
            << Html::Link(navigate_url(false, parent_keys...,  key))
            << "navigate"
            << Html::Link()
            << Html::EndData();
        out << Html::BeginData()
            << Html::Link(tree_url(false, parent_keys...,  key))
            << "tree"
            << Html::Link()
            << Html::EndData();
        out << Html::EndRow();
    }

    out << Html::EndTable();


}

void outputTable(std::ostream& out, const lookup_t& lookup, int listLevel) {

    if (listLevel == 0) {
        outputTableKeys("Databases", out, lookup);
    }

    if (listLevel >= 1) {
        ASSERT(lookup.size() <= 1);
        if (listLevel == 1) {
            outputTableKeys("Index Levels", out, lookup.begin()->second);
        }

        if (listLevel >= 2) {
            ASSERT(lookup.begin()->second.size() <= 1);
            if (lookup.begin()->second.size() == 1) {
                outputTableKeys("Entries", out,
                                lookup.begin()->second.begin()->second,
                                lookup.begin()->first, lookup.begin()->second.begin()->first);
            }
        }
    }
}

} // namespace

//----------------------------------------------------------------------------------------------------------------------

static Navigator navigatorInstance;

Navigator::Navigator() : HttpResource(BASE_URL) {}

Navigator::~Navigator() {}

void Navigator::GET(std::ostream& out, Url& url) {

    auto args = url.json();

    bool doJSON = false;
    if (args.contains("format")) {
        std::string fmt = args.element("format");
        doJSON = (fmt == "json");
        args.remove("format");
    }

    // Construct the request

    auto keys = args.keys();
    ASSERT(keys.isList());
    bool all = (keys.size() == 0);

    metkit::mars::MarsRequest rq("retrieve");

    for (int i = 0; i < keys.size(); ++i) {
        std::string k = keys[i];
        std::string v = args.element(k);
        auto vals = StringTools::split("/", v);
        if (vals.size() != 1) throw UserError("Walking tree only supports single values in request");
        rq.setValuesTyped(new metkit::mars::TypeAny(k), std::move(vals));
    }

    FDBToolRequest tool_request(rq, all, {});

    // How many levels of the schema does this expand (note, it DOES need to use a modified version of full schema
    // expansion, as the exact schema to check against may depend on the DB in question)

    // TODO: If we exactly match the first or second level, use the Axis object rather than
    //       a list retrieval, as an optimisation.

    std::vector<Key> requestBits(3);
    const Schema& schema = Config().expandConfig().schema();
    int listLevel = schema.fullyExpandedLevels(tool_request.request(), requestBits);

    // Our lookup of stuff!
    lookup_t lookup;

    int count = 0;
    FDB fdb;
    bool deduplicate = false;
    auto listObject = fdb.list(tool_request, deduplicate, listLevel+1);
    ListElement elem;
    while (listObject.next(elem)) {

        // Safety check...
        if (count++ > 10000) break;

        auto it = lookup.find(elem.key()[0]);
        if (it == lookup.end()) {
            auto r = lookup.insert(decltype(lookup)::value_type {elem.key()[0], {}});
            ASSERT(r.second);
            it = r.first;
        }

        if (listLevel >= 1) {
            auto& lookup2 = it->second;
            auto it2 = lookup2.find(elem.key()[1]);
            if (it2 == lookup2.end()) {
                auto r = lookup2.insert({elem.key()[1], {}});
                ASSERT(r.second);
                it2 = r.first;
            }

            if (listLevel >= 2) {
                auto& lookup3 = it2->second;
                lookup3.insert(elem.key()[2]);
            }
        }
    }

    if (doJSON) {
        JSON json(out, JSON::Formatting(JSON::Formatting::BitFlags::INDENT_ALL, 3));
        json.startObject();
        json << "query";
        tool_request.request().json(json);
        json << "level" << listLevel;
        outputJSON(json, lookup, listLevel);
        json.endObject();
        out << Html::EndFormatted();
    } else {
        out << Html::BeginBold() << "Search Query: " << Html::EndBold() << tool_request.request() << std::endl;
        out << Html::BeginBold();
        if (listLevel == 0) out << "Partial ";
        if (listLevel >= 0) out << "Database: " << Html::EndBold() << requestBits[0] << Html::BeginBold() << std::endl;
        if (listLevel == 1) out << "Partial ";
        if (listLevel >= 1) out << "Index: " << Html::EndBold() << requestBits[1] << Html::BeginBold() << std::endl;
        if (listLevel == 2) out << "Partial ";
        if (listLevel >= 2) out << "Datum: " << Html::EndBold() << requestBits[2] << Html::BeginBold() << std::endl;
        out << "As JSON: " << Html::EndBold()
            << Html::Link(url.str() + "&format=json") << "here" << Html::Link() << std::endl;

        outputTable(out, lookup, listLevel);
    }


}

//----------------------------------------------------------------------------------------------------------------------

} // namespace httpsvr
} // namespace fdb5
