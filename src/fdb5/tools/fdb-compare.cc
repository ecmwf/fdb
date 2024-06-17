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

class FDBCompare : public FDBVisitTool {

  public: // methods

    FDBCompare(int argc, char **argv) :
        FDBVisitTool(argc, argv, "class,expver") {

        options_.push_back(new SimpleOption<std::string>("testConfig", "Path to a FDB config"));
        options_.push_back(new SimpleOption<std::string>("referenceConfig", "Path to a second FDB config"));
        options_.push_back(new SimpleOption<std::size_t>("level", "The difference can be evaluated at different levels, 1) Mars Metadata (default), 2) Mars and Grib Metadata, 3) Mars and Grib Metadata and data up to a defined tolerance (default bitexact) "));
        options_.push_back(new SimpleOption<double>("tolerance", "Floatinng point tolerance for comparison default=0. Tolerance will only be used if level=3 is set otherwise tolerance will have no effect"));
    }

  private: // methods

    virtual void execute(const CmdArgs& args);
    virtual void init(const CmdArgs &args);

    std::string testConfig_;
    std::string referenceConfig_;
    unsigned int level_;
    double tolerance_;
};


std::string keySignature(const fdb5::Key& key) {
    std::string signature;
    std::string separator;
    for (auto k : key.keys()) {
        signature += separator+k;
        separator=":";
    }
    return signature;
}


void FDBCompare::init(const CmdArgs& args) {

    FDBVisitTool::init(args);


    ASSERT(args.has("testConfig"));
    ASSERT(args.has("referenceConfig"));
    
    testConfig_ = args.getString("testConfig");
    if(testConfig_.empty()){
        throw UserError("No path for FDB 1 specified",Here());
    }
    referenceConfig_ = args.getString("referenceConfig");
    if(referenceConfig_.empty()){
        throw UserError("No path for FDB 2 specified",Here());
    }
    level_ = args.getUnsigned("level",1);
    ASSERT((level_>=1) && (level_<=3));
    tolerance_ = args.getDouble("tolerance",0.0);
    ASSERT(tolerance_>=0);


    /// @todo option ignore-errors
}

void FDBCompare::execute(const CmdArgs& args) {

// from FDBTool.cc
        //     std::string config = args.getString("config", "");
        // if (config.empty()) {
        //     throw eckit::UserError("Missing config file name", Here());
        // }
    PathName configPathtest(testConfig_);
    if (!configPathtest.exists()) {
        std::ostringstream ss;
        ss << "Path " << testConfig_ << " does not exist";
        throw UserError(ss.str(), Here());
    }
    if (configPathtest.isDir()) {
        std::ostringstream ss;
        ss << "Path " << testConfig_ << " is a directory. Expecting a file";
        throw UserError(ss.str(), Here());
    }
   
    FDB fdbtest( Config::make(configPathtest));


// from fdb5/config/Config.h
// class Config : public eckit::LocalConfiguration {
// public:  // static methods
//     static Config make(const eckit::PathName& path, const eckit::Configuration& userConfig = eckit::LocalConfiguration());

// public:  // methods
//     Config();
//     Config(const eckit::Configuration& config, const eckit::Configuration& userConfig = eckit::LocalConfiguration());



// from FDB.h    FDB(const Config& config = Config().expandConfig());
    PathName configPathref(testConfig_);
    if (!configPathref.exists()) {
        std::ostringstream ss;
        ss << "Path " << referenceConfig_ << " does not exist";
        throw UserError(ss.str(), Here());
    }
    if (configPathref.isDir()) {
        std::ostringstream ss;
        ss << "Path " << referenceConfig_ << " is a directory. Expecting a file";
        throw UserError(ss.str(), Here());
    }
    
    FDB fdbref( Config::make(configPathref));


    // std::unique_ptr<JSON> json;
    // if (json_) {
    //     json.reset(new JSON(Log::info()));
    //     json->startList();
    // }

    // for (const FDBToolRequest& request : requests()) {

    //     if (!porcelain_) {
    //         Log::info() << "Listing for request" << std::endl;
    //         request.print(Log::info());
    //         Log::info() << std::endl;
    //     }

    //     // If --full is supplied, then include all entries including duplicates.
    //     auto listObject = fdb.list(request, !full_ && !compact_);
    //     std::map<std::string, std::map<std::string, std::pair<metkit::mars::MarsRequest, std::unordered_set<Key>>>> requests;

    //     ListElement elem;
    //     while (listObject.next(elem)) {

    //         if (compact_) {
    //             std::vector<Key> keys = elem.key();
    //             ASSERT(keys.size() == 3);

    //             std::string treeAxes = keys[0];
    //             treeAxes += ",";
    //             treeAxes += keys[1];

    //             std::string signature=keySignature(keys[2]);  // i.e. step:levelist:param

    //             auto it = requests.find(treeAxes);
    //             if (it == requests.end()) {
    //                 std::map<std::string, std::pair<metkit::mars::MarsRequest, std::unordered_set<Key>>> leaves;
    //                 leaves.emplace(signature, std::make_pair(keys[2].request(), std::unordered_set<Key>{keys[2]}));
    //                 requests.emplace(treeAxes, leaves);
    //             } else {
    //                 auto h = it->second.find(signature);
    //                 if (h != it->second.end()) { // the hypercube request is already there... adding the 3rd level key
    //                     h->second.first.merge(keys[2].request());
    //                     h->second.second.insert(keys[2]);
    //                 } else {
    //                     it->second.emplace(signature, std::make_pair(keys[2].request(), std::unordered_set<Key>{keys[2]}));
    //                 }
    //             }
    //         } else {
    //             if (json_) {
    //                 (*json) << elem;
    //             } else {
    //                 elem.print(Log::info(), location_, !porcelain_);
    //                 Log::info() << std::endl;
    //             }
    //         }
    //     }
    //     if (compact_) {
    //         for (const auto& tree: requests) {
    //             for (const auto& leaf: tree.second) {
    //                 metkit::hypercube::HyperCube h{leaf.second.first};
    //                 if (h.size() == leaf.second.second.size()) {
    //                     Log::info() << "retrieve," << tree.first << ",";
    //                     leaf.second.first.dump(Log::info(), "", "", false);
    //                     Log::info() << std::endl;
    //                 } else {
    //                     for (const auto& k: leaf.second.second) {
    //                         h.clear(k.request());
    //                     }
    //                     for (const auto& r: h.requests()) {
    //                         Log::info() << "retrieve," << tree.first << ",";
    //                         r.dump(Log::info(), "", "", false);
    //                         Log::info() << std::endl;
    //                     }
    //                 }
    //             }
    //         }
    //     }
    //     // n.b. finding no data is not an error for fdb-list
    // }

    // if (json_) {
    //     json->endList();
    // }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace tools
} // namespace fdb5

int main(int argc, char **argv) {
    fdb5::tools::FDBCompare app(argc, argv);
    return app.start();
}

