/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <unordered_set>

#include "eckit/option/CmdArgs.h"
#include "eckit/config/Resource.h"
#include "eckit/option/SimpleOption.h"
#include "eckit/option/VectorOption.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/log/JSON.h"

#include "metkit/hypercube/HyperCube.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/database/DB.h"
#include "fdb5/database/Index.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/tools/FDBVisitTool.h"

using namespace eckit;
using namespace eckit::option;

namespace fdb5 {
namespace tools {

//----------------------------------------------------------------------------------------------------------------------

class FDBList : public FDBVisitTool {

  public: // methods

    FDBList(int argc, char **argv) :
        FDBVisitTool(argc, argv, "class,expver"),
        location_(false),
        full_(false),
        porcelain_(false),
        json_(false) {

        options_.push_back(new SimpleOption<bool>("location", "Also print the location of each field"));
        options_.push_back(new SimpleOption<bool>("full", "Include all entries (including masked duplicates)"));
        options_.push_back(new SimpleOption<bool>("porcelain", "Streamlined and stable output for input into other tools"));
        options_.push_back(new SimpleOption<bool>("json", "Output available fields in JSON form"));
        options_.push_back(new SimpleOption<bool>("compact", "Aggregate available fields in MARS requests"));
    }

  private: // methods

    virtual void execute(const CmdArgs& args);
    virtual void init(const CmdArgs &args);

    bool location_;
    bool full_;
    bool porcelain_;
    bool json_;
    bool compact_;
};

void FDBList::init(const CmdArgs& args) {

    FDBVisitTool::init(args);

    location_ = args.getBool("location", false);
    full_ = args.getBool("full", false);
    porcelain_ = args.getBool("porcelain", false);
    json_ = args.getBool("json", false);
    compact_ = args.getBool("compact", false);

    if (json_) {
        porcelain_ = true;
        if (location_) {
            throw UserError("--json and --location are not compatible", Here());
        }
    }

    if (compact_) {
        if (location_) {
            throw UserError("--compact and --location are not compatible", Here());
        } 
        if (full_) {
            throw UserError("--compact and --full are not compatible", Here());
        } 
        if (porcelain_) {
            throw UserError("--compact and --porcelain are not compatible", Here());
        } 
    }

    /// @todo option ignore-errors
}

std::string keySignature(const fdb5::Key& key) {
    std::string signature;
    std::string separator="";
    for (auto k : key.keys()) {
        signature += separator+k;
        separator=":";
    }
    return signature;
}

void FDBList::execute(const CmdArgs& args) {

    FDB fdb(config(args));

    std::unique_ptr<JSON> json;
    if (json_) {
        json.reset(new JSON(Log::info()));
        json->startList();
    }

    for (const FDBToolRequest& request : requests()) {

        if (!porcelain_) {
            Log::info() << "Listing for request" << std::endl;
            request.print(Log::info());
            Log::info() << std::endl;
        }

        // If --full is supplied, then include all entries including duplicates.
        auto listObject = fdb.list(request, !full_ && !compact_);
        std::unordered_set<Key> seenKeys;
        std::map<std::string, metkit::mars::MarsRequest> requests;

        size_t count = 0;
        ListElement elem;
        while (listObject.next(elem)) {

            if (compact_) {
                fdb5::Key combined = elem.combinedKey();
                std::string axes = keySignature(combined);
                auto it = requests.find(axes);
                if (it == requests.end()) {
                    requests.emplace(axes, combined.request());
                } else {
                    it->second.merge(combined.request());
                }
                seenKeys.emplace(combined);
            } else {
                if (json_) {
                    (*json) << elem;
                } else {
                    elem.print(Log::info(), location_, !porcelain_);
                    Log::info() << std::endl;
                    count++;
                }
            }
        }
        if (compact_) {
            std::map<std::string, metkit::hypercube::HyperCube*> hypercubes;

            for (auto r: requests) {
                hypercubes.emplace(r.first, new metkit::hypercube::HyperCube(r.second));
            }
            for (auto k: seenKeys) {
                auto it = hypercubes.find(keySignature(k));
                ASSERT(it != hypercubes.end());
                it->second->clear(k.request());
            }
            for (auto h: hypercubes) {
                for (auto r: h.second->requests()) {
                    r.dump(Log::info(), "", "");
                    Log::info() << std::endl;
                }
            }
        }

        // n.b. finding no data is not an error for fdb-list
    }

    if (json_) {
        json->endList();
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace tools
} // namespace fdb5

int main(int argc, char **argv) {
    fdb5::tools::FDBList app(argc, argv);
    return app.start();
}

