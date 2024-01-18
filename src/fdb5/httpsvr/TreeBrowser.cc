/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/httpsvr/TreeBrowser.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/rules/Predicate.h"
#include "fdb5/rules/Rule.h"
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

const std::string BASE_URL("/fdbtree");

std::string construct_url(const metkit::mars::MarsRequest& base_request,
                          const std::string& key,
                          const std::string& value,
                          bool doJSON) {

    std::string rq_bits;
    for (const auto& param : base_request.params()) {
        std::vector<std::string> vals;
        base_request.getValues(param, vals);
        ASSERT(vals.size() == 1);
        rq_bits += param + "=" + vals[0] + "&";
    }

    std::string url = BASE_URL + "?"
                    + rq_bits
                    + (key + "=" + value)
                    + (doJSON ? "&format=json" : "");

    return url;
}

} // namespace

//----------------------------------------------------------------------------------------------------------------------

static TreeBrowser treeBrowserInstance;

TreeBrowser::TreeBrowser() : HttpResource(BASE_URL) {}

TreeBrowser::~TreeBrowser() {}

void TreeBrowser::GET(std::ostream& out, Url& url) {

    eckit::Timer trequets("tree.request");
    eckit::Timer tprepare("tree.prepare");

    // TODO: Break this up into sub-functions, and add some tests.

    // Parse arguments

    auto args = url.json();

    bool raw = true;
//    if (args.contains("raw")) {
//        args.remove("raw");
//        raw = true;
//    }

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

    // See the same logic as for FDBToolRequest
//    if (!raw) {
//        bool inherit = false;
//        bool strict = true;
//        metkit::mars::MarsExpension expand(inherit, strict);
//
//        auto expandedRequest = expand.expand(rq);
//        out << expandedRequest << std::endl;
//    }

    FDBToolRequest tool_request(rq, all, {});

    tprepare.stop();

    Config config = Config().expandConfig();
    const Schema& schema = config.schema();

    out << "TOOL request: " << rq << std::endl;

    // How many levels of the schema does this expand (note, it DOES need to use a modified version of full schema
    // expansion, as the exact schema to check against may depend on the DB in question)

    // TODO: If we exactly match the first or second level, use the Axis object rather than
    //       a list retrieval, as an optimisation.

    eckit::Timer tlevels("tree.levels");
    std::vector<Key> requestBits(3);
    std::vector<std::string> nextKeys;
    int listLevel = schema.fullyExpandedLevels(tool_request.request(), requestBits, &nextKeys);
    tlevels.stop();

    out << "listLevel: " << listLevel << std::endl;
    out << "requestBits: " << requestBits[0] << requestBits[1] << requestBits[2] << std::endl;
    out << "nextKeys: " << nextKeys << std::endl;

    // List to the requisite level, and then see what is left. n.b. there may (in principle) be multiple
    // keys to consider, as there may be multiple sub-rules per matched rule.

    std::map<std::string, std::set<std::string>> nextVals;

    FDB fdb;

    if (listLevel > 0 && listLevel < 3 && requestBits[listLevel].empty()) {

        // We have exactly matched
//        eckit::Timer taxes("fdb.axes");
        auto axes = fdb.axes(tool_request);
//        taxes.stop();

        // This is a little bit of a hack, as number is better for subsetting the operational data
        if (listLevel == 2 && nextKeys.size() == 1 && nextKeys[0] == "step") {
            nextKeys = {"number"};
        }

        for (const auto& key : nextKeys) {
            if (nextVals.find(key) == nextVals.end()) {
                auto r = nextVals.emplace(key, std::set<std::string>{});
                for (const auto& v : axes.values(key)) {
                    r.first->second.insert(v);
                }
            }
        }
    } else {

//        eckit::Timer tlist("fdb.list");

        bool deduplicate = false;
        auto listObject = fdb.list(tool_request, deduplicate, listLevel + 1);
        ListElement elem;
        while (listObject.next(elem)) {

            // For each of the elements, consider the combinedKey, and walk it in the
            // schema-related order. Then find the next element.

            int i = 0;
            Key combinedKey = elem.combinedKey(/* ordered */ true);
            for (const auto& k: combinedKey.names()) {
                if (!tool_request.request().has(k)) {
                    auto r = nextVals.try_emplace(k, std::set<std::string>{});
                    r.first->second.insert(combinedKey.get(k));
                    break;
                }
                /*
                if (i++ < keys.size()) {
                    // Check that the keys are in order (given this is a tree browser)
                    if (k != "step") {
                        ASSERT(tool_request.request().has(k));
                    }
                } else {
                    auto r = nextVals.try_emplace(k, std::set<std::string>{});
                    r.first->second.insert(combinedKey.get(k));
                    break;
                }
                 */
            }
        }
    }

    // Output this nicely.

    if (doJSON) {
        JSON json(out);
        json.startObject();
        json << "query" << tool_request.request();
        json << "parsed-query" << requestBits;
        json << "keys";
        json.startList();
        for (const auto& kv: nextVals) {
            json.startObject();
            json << "name" << kv.first;
            json << "values";
            json.startList();
            for (const auto& v: kv.second) json << v;
            json.endList();
            json << "links";
            json.startList();
            for (const auto& v: kv.second) json << construct_url(rq, kv.first, v, doJSON);
            json.endList();
            json.endObject();
        }
        json.endList();
        json.endObject();
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

        for (const auto& kv: nextVals) {
            out << Html::BeginH1() << kv.first << Html::EndH1();
            for (const auto& v: kv.second) {
                out << Html::Link(construct_url(rq, kv.first, v, doJSON))
                    << v
                    << Html::Link() << std::endl;
            }
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace httpsvr
} // namespace fdb5
