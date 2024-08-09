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
#include "eckit/option/SimpleOption.h"
#include "eckit/option/CmdArgs.h"
#include "fdb5/api/FDB.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/tools/FDBTool.h"
#include "fdb5/tools/FDBVisitTool.h"

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

std::atomic<std::size_t> count {0};

void inspect(const Config& config, const FDBToolRequest& toolRequest) {
    FDB fdb(config);

    auto iter = fdb.inspect(toolRequest.request());

    ListElement elem;
    while (iter.next(elem)) { ++count; }
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

class FDBInspectTool: public FDBVisitTool {
public:  // methods
    FDBInspectTool(int argc, char** argv): FDBVisitTool(argc, argv, "") {
        options_.push_back(new eckit::option::SimpleOption<std::size_t>("parallel", "Number of parallel tasks to run"));
    }

private:  // methods
    void init(const eckit::option::CmdArgs& args) override;

    void execute(const eckit::option::CmdArgs& args) override;

    std::size_t parallel_ {1};
};

//----------------------------------------------------------------------------------------------------------------------

void FDBInspectTool::init(const eckit::option::CmdArgs& args) {
    FDBVisitTool::init(args);

    parallel_ = args.getUnsigned("parallel", parallel_);

    if (parallel_ < 1) { throw eckit::UserError("Number of parallel tasks must be greater than 0"); }
}

//----------------------------------------------------------------------------------------------------------------------

void FDBInspectTool::execute(const eckit::option::CmdArgs& args) {
    const auto fdbConfig    = config(args);
    const auto toolRequests = requests();

    if (parallel_ == 1) {
        for (const auto& request : toolRequests) { inspect(fdbConfig, request); }
    } else {
        using req_iter = std::vector<FDBToolRequest>::const_iterator;

        const auto inspectFn = [&fdbConfig](req_iter begin, req_iter end) {
            for (auto req = begin; req != end; ++req) { inspect(fdbConfig, *req); }
        };

        if (toolRequests.size() < parallel_) { parallel_ = toolRequests.size(); }

        const auto workPerThread = toolRequests.size() / parallel_;

        std::vector<std::thread> threads(parallel_);

        auto reqIter = toolRequests.begin();
        for (auto it = threads.begin(); it != threads.end() - 1; ++it) {
            *it      = std::thread(inspectFn, reqIter, reqIter + workPerThread);
            reqIter += workPerThread;
        }
        threads.back() = std::thread(inspectFn, reqIter, toolRequests.end());

        for (auto&& thread : threads) { thread.join(); }
    }

    eckit::Log::info() << "Total number of elements: " << count << '\n';
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::tools

int main(int argc, char** argv) {
    fdb5::tools::FDBInspectTool app(argc, argv);
    return app.start();
}
