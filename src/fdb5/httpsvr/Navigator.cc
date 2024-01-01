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

using lookup_t = std::unordered_map<Key, std::unordered_map<Key, std::set<Key>>>;

void outputJSON(JSON& json, const lookup_t& lookup, int listLevel) {
    json << "databases";
    json.startList();
    for (const auto& db_kv : lookup) {
        json.startObject();
        json << "key" << db_kv.first;

        if (listLevel > 1) {
            json << "indexes";
            json.startList();
            for (const auto& idx_kv : db_kv.second) {
                json << "key" << idx_kv.first;
            }
            json.endList();
        }
    }
    json.endList();
}

} // namespace

//----------------------------------------------------------------------------------------------------------------------

static Navigator navigatorInstance;

Navigator::Navigator() : HttpResource("/fdbnavigator") {}

Navigator::~Navigator() {}

void Navigator::GET(std::ostream& out, Url& url) {
    out << "Hello World!" << std::endl;
    out << url << std::endl;
    out << url.json() << std::endl;

    auto args = url.json();

    bool raw = false;
    if (args.contains("raw")) {
        args.remove("raw");
        raw = true;
    }

    bool doJSON = false;
    if (args.contains("format")) {
        std::string fmt = args.element("format");
        doJSON = (fmt == "json");
        args.remove("format");
    }

    auto keys = args.keys();
    ASSERT(keys.isList());
    bool all = (keys.size() == 0);

    metkit::mars::MarsRequest rq("retrieve");

    for (int i = 0; i < keys.size(); ++i) {
        std::string k = keys[i];
        std::string v = args.element(k);
        rq.setValuesTyped(new metkit::mars::TypeAny(k), StringTools::split("/", v));
    }

    out << rq << std::endl;
    out << "json=" << (doJSON?"T":"F") << std::endl;
    out << "raw=" << (raw?"T":"F") << std::endl;

    // See the same logic as for FDBToolRequest
    if (!raw) {
        bool inherit = false;
        bool strict = true;
        metkit::mars::MarsExpension expand(inherit, strict);

        auto expandedRequest = expand.expand(rq);
        out << expandedRequest << std::endl;
    }

    FDBToolRequest tool_request(rq, all, {});
    out << tool_request << std::endl;

    // Test expanding the first level of the schema. Are we doing sub-DB listing?

    bool foundFirstLevel = false;
    try {
        const Schema& schema = Config().expandConfig().schema();
        std::vector<Key> result;
        foundFirstLevel = schema.expandFirstLevel(tool_request.request(), result);
//        for (const auto& k : result) {
//            out << " >> " << k << std::endl;
//        }
    } catch (const UserError&) {
        // TODO: expandFirstLevel clearly not doing what we want. This is throwing an exception
        //       rather than returning found=false if a key is completely missing. But we should
        //       be continuing to explore the schema (there is no reason that a key should be
        //       REQUIRED at any PARTICULAR point whilst exploring the schema).
        // foundFirstLevel already is false. Ignore.
    }

    int listLevel = 1;
    if (foundFirstLevel) {
        listLevel = 2;
    }

    std::unique_ptr<JSON> json;
    if (doJSON) {
//        json = std::make_unique<JSON>(out);
        out << Html::BeginFormatted();
        json = std::make_unique<JSON>(out, JSON::Formatting(JSON::Formatting::BitFlags::INDENT_ALL, 3));
        json->startObject();
        (*json) << "query";
        tool_request.request().json(*json);
        (*json) << "level" << listLevel;
        (*json) << "raw" << raw;
    }

    // Our lookup of stuff!
    lookup_t lookup;

    FDB fdb;
    bool deduplicate = false;
    auto listObject = fdb.list(tool_request, deduplicate, listLevel);
    ListElement elem;
    while (listObject.next(elem)) {

        auto it = lookup.find(elem.key()[0]);
        if (it == lookup.end()) {
            auto r = lookup.insert(decltype(lookup)::value_type {elem.key()[0], {}});
            ASSERT(r.second);
            it = r.first;
        }

        if (listLevel > 1) {
            auto& lookup2 = it->second;
            auto it2 = lookup2.find(elem.key()[1]);
            if (it2 == lookup2.end()) {
                auto r = lookup2.insert({elem.key()[1], {}});
                ASSERT(r.second);
                it2 = r.first;
            }
        }
    }

//    out << Html::BeginTable(true);
//    out << Html::BeginRow();
//    out << Html::BeginHeader() << "boo" << Html::EndHeader();
//    out << Html::BeginHeader() << "too" << Html::EndHeader();
//    out << Html::EndRow();
//    out << Html::EndTable();

//    for (const auto& db_kv : lookup) {
//        out << Html::BeginBold()
//            << "Database: " << db_kv.first
//            << Html::EndBold() << std::endl;
//        for (const auto& idx_kv : db_kv.second) {
//            out << idx_kv.first << std::endl;
//        }
//    }
    if (doJSON) {
        outputJSON(*json, lookup, listLevel);
        json->endObject();
        out << Html::EndFormatted();
    } else {
        NOTIMP;
    }


}

//----------------------------------------------------------------------------------------------------------------------

} // namespace httpsvr
} // namespace fdb5
