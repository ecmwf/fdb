/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "eckit/config/Resource.h"
#include "eckit/option/SimpleOption.h"
#include "eckit/option/VectorOption.h"
#include "eckit/option/CmdArgs.h"

#include "metkit/MarsParser.h"
#include "metkit/MarsExpension.h"

#include "fdb5/tools/FDBVisitTool.h"


using namespace eckit;
using namespace eckit::option;

namespace fdb5 {
namespace tools {

//----------------------------------------------------------------------------------------------------------------------


FDBVisitTool::FDBVisitTool(int argc, char **argv, std::string minimumKeys) :
    FDBTool(argc, argv),
    fail_(true),
    all_(false) {

    minimumKeys_ = Resource<std::vector<std::string> >("FDBInspectMinimumKeys", minimumKeys, true);

    if(minimumKeys_.size() == 0) {
        options_.push_back(new SimpleOption<bool>("all", "Visit all FDB databases"));
    } else {
        options_.push_back(new VectorOption<std::string>("minimum-keys",
                                                         "Use these keywords as a minimun set which *must* be specified",
                                                         0, ","));
    }

    // Be able to turn ignore-errors off
    options_.push_back(
                new SimpleOption<bool>("ignore-errors",
                                       "Ignore errors (report them as warnings) and continue processing wherever possible"));

    // Bypass the check for minimum keys
    options_.push_back(new SimpleOption<bool>("force", "Bypass the check for minimum keys"));
}

FDBVisitTool::~FDBVisitTool() {}

void FDBVisitTool::init(const option::CmdArgs& args) {

    FDBTool::init(args);

    args.get("minimum-keys", minimumKeys_);

    bool ignore = false;;
    args.get("ignore-errors", ignore);
    fail_ = !ignore;

    args.get("all", all_);

    if (all_ && args.count()) {
        usage(args.tool());
        exit(1);
    }

    for (size_t i = 0; i < args.count(); ++i) {
        requests_.emplace_back(args(i));
    }

    if (all_) {
        ASSERT(requests_.empty());
    }
}

bool FDBVisitTool::fail() const {
    return fail_;
}

std::vector<FDBToolRequest> FDBVisitTool::requests() const {

    std::vector<FDBToolRequest> requests;

    if (all_) {
        ASSERT(requests_.empty());
        requests.emplace_back(FDBToolRequest(metkit::MarsRequest{}, all_, minimumKeys_));
    } else {

        for (const std::string& request_string : requests_) {
            auto parsed = FDBToolRequest::requestsFromString(request_string, minimumKeys_);
            requests.insert(requests.end(), parsed.begin(), parsed.end());
        }
    }

    return requests;
}

void FDBVisitTool::usage(const std::string &tool) const {

               // derived classes should provide this type of usage information ...

                //                       << "Usage: " << tool << " [options] [path1|request1] [path2|request2] ..." << std::endl
                //                       << std::endl
                //                       << std::endl

    Log::info() << std::endl
                << "Examples:" << std::endl
                << "=========" << std::endl << std::endl
                << tool << " ."
                << std::endl
                << tool << " /tmp/fdb/od:0001:oper:20160428:1200:g"
                << std::endl
                << tool << " class=od,expver=0001,stream=oper,date=20160428,time=1200,domain=g"
                << std::endl
                << std::endl;

    FDBTool::usage(tool);
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace tools
} // namespace fdb5
