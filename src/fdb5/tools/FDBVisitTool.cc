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
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"
#include "eckit/option/VectorOption.h"

#include "metkit/mars/MarsExpansion.h"
#include "metkit/mars/MarsParser.h"
#include "metkit/mars/MarsRequest.h"

#include "fdb5/tools/FDBVisitTool.h"


using namespace eckit;
using namespace eckit::option;

namespace fdb5 {
namespace tools {

//----------------------------------------------------------------------------------------------------------------------


FDBVisitTool::FDBVisitTool(int argc, char** argv, std::string minimumKeys) :
    FDBTool(argc, argv), fail_(true), all_(false), raw_(false) {

    minimumKeys_ = Resource<std::vector<std::string>>("FDBInspectMinimumKeys", minimumKeys, true);

    if (minimumKeys_.size() != 0) {
        options_.push_back(new VectorOption<std::string>(
            "minimum-keys", "Use these keywords as a minimum set which *must* be specified", 0, ","));
    }

    // Don't apply MarsExpansion to the parsed requests. This relies on the user
    // to provide values that are acceptable internally
    options_.push_back(new SimpleOption<bool>("raw", "Don't apply (contextual) expansion and checking on requests."));

    // Be able to turn ignore-errors off
    options_.push_back(new SimpleOption<bool>(
        "ignore-errors", "Ignore errors (report them as warnings) and continue processing wherever possible"));
}

FDBVisitTool::~FDBVisitTool() {}

void FDBVisitTool::run() {

    // We add this option here. This creates a hidden option that can be used by advanced users
    // (i.e. in unit tests/debugging) but is not visible to users.
    options_.push_back(new SimpleOption<bool>("all", "Visit all FDB databases"));

    FDBTool::run();
}

void FDBVisitTool::init(const option::CmdArgs& args) {

    FDBTool::init(args);

    args.get("minimum-keys", minimumKeys_);

    bool ignore = args.getBool("ignore-errors", false);
    fail_ = !ignore;

    all_ = args.getBool("all", false);

    if (!minimumKeys_.empty() && all_) {
        throw eckit::UserError("--all option (advanced for debugging) can only be used with no minimum keys", Here());
    }

    raw_ = args.getBool("raw", false);

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

std::vector<FDBToolRequest> FDBVisitTool::requests(const std::string& verb) const {

    std::vector<FDBToolRequest> requests;

    if (all_) {
        ASSERT(requests_.empty());
        requests.emplace_back(metkit::mars::MarsRequest{}, all_, minimumKeys_);
    }
    else {

        for (const std::string& request_string : requests_) {
            auto parsed = FDBToolRequest::requestsFromString(request_string, minimumKeys_, raw_, verb);
            requests.insert(requests.end(), parsed.begin(), parsed.end());
        }
    }

    return requests;
}

void FDBVisitTool::usage(const std::string& tool) const {

    // derived classes should provide this type of usage information ...

    Log::info() << "Usage: " << tool << " [options] [request1] [request2] ..." << std::endl << std::endl;

    Log::info() << "Examples:" << std::endl
                << "=========" << std::endl
                << std::endl
                << tool << " class=rd,expver=xywz,stream=oper,date=20190603,time=00" << std::endl
                << std::endl;

    FDBTool::usage(tool);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace tools
}  // namespace fdb5
