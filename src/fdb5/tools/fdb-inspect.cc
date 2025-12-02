/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   fdb-inspect.cc
/// @author Metin Cakircali
/// @date   Aug 2024

#include "eckit/exception/Exceptions.h"
#include "eckit/log/Log.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/tools/FDBTool.h"
#include "fdb5/tools/FDBVisitTool.h"
#include "metkit/mars/MarsRequest.h"

#include <atomic>
#include <cstddef>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace fdb5::tools {

//----------------------------------------------------------------------------------------------------------------------
// HELPER FUNCTIONS

namespace {

std::atomic<std::size_t> elemCount{0};

using metkit::mars::MarsRequest;

void inspect(const Config& config, const MarsRequest& request, bool output) {
    auto iter = FDB(config).inspect(request);

    ListElement elem;
    while (iter.next(elem)) {
        ++elemCount;
        if (output) {
            elem.print(eckit::Log::info(), true, false, true, ", ");
            eckit::Log::info() << std::endl;
        }
    }
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

class FDBInspectTool : public FDBVisitTool {
public:  // methods

    FDBInspectTool(int argc, char** argv) : FDBVisitTool(argc, argv, "") {
        using eckit::option::SimpleOption;
        options_.push_back(new SimpleOption<bool>("output", "Print the output of the inspection"));
        options_.push_back(new SimpleOption<std::size_t>("parallel", "Number of parallel tasks to run"));
    }

private:  // methods

    void init(const eckit::option::CmdArgs& args) override;

    void execute(const eckit::option::CmdArgs& args) override;

    bool output_{false};

    std::size_t parallel_{1};
};

//----------------------------------------------------------------------------------------------------------------------

void FDBInspectTool::init(const eckit::option::CmdArgs& args) {
    FDBVisitTool::init(args);

    output_ = args.getBool("output", output_);

    parallel_ = args.getUnsigned("parallel", parallel_);

    if (parallel_ < 1) {
        throw eckit::UserError("Number of parallel tasks must be greater than 0");
    }
}

//----------------------------------------------------------------------------------------------------------------------

void printHeader(const FDBToolRequest& request) {
    eckit::Log::info() << "Inspecting request: " << request << std::endl;
}

void FDBInspectTool::execute(const eckit::option::CmdArgs& args) {
    const auto fdbConfig = config(args);
    const auto toolRequests = requests();

    eckit::Log::info() << "Number of requests: " << toolRequests.size() << std::endl;

    if (parallel_ == 1) {
        for (const auto& req : toolRequests) {
            inspect(fdbConfig, req.request(), output_);
        }
    }
    else {
        using req_iter_t = std::vector<FDBToolRequest>::const_iterator;

        const auto inspectFn = [&](req_iter_t begin, req_iter_t end) {
            for (auto reqIter = begin; reqIter != end; ++reqIter) {
                inspect(fdbConfig, reqIter->request(), output_);
            }
        };

        if (toolRequests.size() < parallel_) {
            parallel_ = toolRequests.size();
        }

        const auto requestPerThread = toolRequests.size() / parallel_;

        std::vector<std::thread> threads(parallel_);

        auto reqIter = toolRequests.begin();
        for (auto iter = threads.begin(); iter != threads.end() - 1; ++iter) {
            *iter = std::thread(inspectFn, reqIter, reqIter + requestPerThread);
            reqIter += requestPerThread;
        }
        threads.back() = std::thread(inspectFn, reqIter, toolRequests.end());

        for (auto&& thread : threads) {
            thread.join();
        }
    }

    eckit::Log::info() << "Number of elements: " << elemCount << '\n';
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::tools

int main(int argc, char** argv) {
    fdb5::tools::FDBInspectTool app(argc, argv);
    return app.start();
}
