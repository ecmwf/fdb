/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "eckit/exception/Exceptions.h"
#include "eckit/log/JSON.h"
#include "eckit/log/Log.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"

#include "fdb5/LibFdb5.h"
#include "metkit/hypercube/HyperCube.h"
#include "metkit/mars/MarsRequest.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/database/Index.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/tools/FDBVisitTool.h"

using namespace eckit;
using namespace eckit::option;

namespace fdb5::tools {

//----------------------------------------------------------------------------------------------------------------------

class FDBList : public FDBVisitTool {
public:  // methods

    FDBList(int argc, char** argv) : FDBVisitTool(argc, argv, "class,expver") {
        options_.push_back(new SimpleOption<bool>("location", "Also print the location of each field"));
        options_.push_back(new SimpleOption<bool>("timestamp", "Also print the timestamp when the field was indexed"));
        options_.push_back(new SimpleOption<bool>("length", "Also print the field size"));
        options_.push_back(new SimpleOption<bool>("full", "Include all entries (including masked duplicates)"));
        options_.push_back(
            new SimpleOption<bool>("porcelain",
                                   "Streamlined and stable output. Useful as input for other tools or scripts."
                                   "Incompatible with options: location, timestamp, and length"));
        options_.push_back(new SimpleOption<bool>("json", "Output available fields in JSON form"));
        options_.push_back(new SimpleOption<bool>("compact", "Aggregate available fields in MARS requests"));
        options_.push_back(new SimpleOption<long>("depth", "Output entries up to 'depth' levels deep [1-3]"));
    }

private:  // methods

    void execute(const CmdArgs& args) override;
    void init(const CmdArgs& args) override;

    bool location_{false};
    bool timestamp_{false};
    bool length_{false};
    bool full_{false};
    bool porcelain_{false};
    bool json_{false};
    bool compact_{false};
    int depth_{3};
};

//----------------------------------------------------------------------------------------------------------------------

static std::string keySignature(const fdb5::Key& key) {
    std::string signature;
    std::string separator;
    for (auto&& k : key.keys()) {
        signature += separator + k;
        separator = ":";
    }
    return signature;
}

void FDBList::init(const CmdArgs& args) {

    FDBVisitTool::init(args);

    location_  = args.getBool("location", location_);
    timestamp_ = args.getBool("timestamp", timestamp_);
    length_    = args.getBool("length", length_);
    full_      = args.getBool("full", full_);
    porcelain_ = args.getBool("porcelain", porcelain_);
    json_      = args.getBool("json", json_);
    compact_   = args.getBool("compact", compact_);
    depth_     = args.getInt("depth", depth_);

    ASSERT(depth_ > 0 && depth_ < 4);

    if (json_) {
        eckit::Log::debug<LibFdb5>() << "Setting porcelain=true" << '\n';
        porcelain_ = true;
    }

    if (porcelain_) {
        if (location_) {
            throw UserError("--porcelain and --location are not compatible", Here());
        }
        if (timestamp_) {
            throw UserError("--porcelain and --timestamp are not compatible", Here());
        }
        if (length_) {
            throw UserError("--porcelain and --length are not compatible", Here());
        }
        if (compact_) {
            throw UserError("--porcelain and --compact are not compatible", Here());
        }
    }

    if (compact_) {
        if (location_) {
            throw UserError("--compact and --location are not compatible", Here());
        }
        if (full_) {
            throw UserError("--compact and --full are not compatible", Here());
        }
    }

    /// @todo option ignore-errors
}

void FDBList::execute(const CmdArgs& args) {

    FDB fdb(config(args));

    std::unique_ptr<JSON> json;
    if (json_) {
        json = std::make_unique<JSON>(Log::info());
        json->startList();
    }

    for (const FDBToolRequest& request : requests()) {

        if (!porcelain_) {
            Log::info() << "Listing for request" << std::endl;
            request.print(Log::info());
            Log::info() << std::endl;
        }

        // If --full is supplied, then include all entries including duplicates.
        auto listObject = fdb.list(request, !full_ && !compact_, depth_);
        std::map<std::string, std::map<std::string, std::pair<metkit::mars::MarsRequest, std::unordered_set<Key>>>>
            requests;

        ListElement elem;
        while (listObject.next(elem)) {

            if (compact_) {
                const auto& keys = elem.keys();
                ASSERT(keys.size() == 3);

                std::string treeAxes = keys[0];
                treeAxes += ",";
                treeAxes += keys[1];

                std::string signature = keySignature(keys[2]);  // i.e. step:levelist:param

                auto it = requests.find(treeAxes);
                if (it == requests.end()) {
                    std::map<std::string, std::pair<metkit::mars::MarsRequest, std::unordered_set<Key>>> leaves;
                    leaves.emplace(signature, std::make_pair(keys[2].request(), std::unordered_set<Key>{keys[2]}));
                    requests.emplace(treeAxes, leaves);
                }
                else {
                    auto h = it->second.find(signature);
                    if (h != it->second.end()) {  // the hypercube request is already there... adding the 3rd level key
                        h->second.first.merge(keys[2].request());
                        h->second.second.insert(keys[2]);
                    }
                    else {
                        it->second.emplace(signature,
                                           std::make_pair(keys[2].request(), std::unordered_set<Key>{keys[2]}));
                    }
                }
                continue;
            }

            // JSON output
            if (json) {
                *json << elem;
                continue;
            }

            elem.print(Log::info(), location_, length_, timestamp_, ", ");
            Log::info() << std::endl;

        }  // while

        if (compact_) {
            for (const auto& tree : requests) {
                for (const auto& leaf : tree.second) {
                    metkit::hypercube::HyperCube h{leaf.second.first};
                    if (h.size() == leaf.second.second.size()) {
                        Log::info() << "retrieve," << tree.first << ",";
                        leaf.second.first.dump(Log::info(), "", "", false);
                        Log::info() << std::endl;
                    }
                    else {
                        for (const auto& k : leaf.second.second) {
                            h.clear(k.request());
                        }
                        for (const auto& r : h.requests()) {
                            Log::info() << "retrieve," << tree.first << ",";
                            r.dump(Log::info(), "", "", false);
                            Log::info() << std::endl;
                        }
                    }
                }
            }
        }
        // n.b. finding no data is not an error for fdb-list

    }  // requests

    if (json) {
        json->endList();
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::tools

int main(int argc, char** argv) {
    fdb5::tools::FDBList app(argc, argv);
    return app.start();
}
