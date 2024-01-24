/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"
#include "eckit/log/JSON.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/tools/FDBVisitTool.h"
#include "fdb5/LibFdb5.h"

#include <thread>
#include <future>

using namespace eckit;
using namespace option;


namespace fdb5 {
namespace tools {

//----------------------------------------------------------------------------------------------------------------------

class FDBInspectTest : public FDBVisitTool {

public: // methods

    FDBInspectTest(int argc, char **argv) :
            FDBVisitTool(argc, argv, "class,expver") {
        options_.push_back(new SimpleOption<long>("instances", "Number of copies to run in parallel in threads"));
    }

private: // methods

    void execute(const CmdArgs& args) override;
    void init(const CmdArgs &args) override;

private: // members

    long instances_ = 1;
};

void FDBInspectTest::init(const CmdArgs& args) {
    FDBVisitTool::init(args);
    instances_ = args.getLong("instances", instances_);
}

void FDBInspectTest::execute(const CmdArgs& args) {

    auto workFn = [&, this] {

        FDB fdb(config(args));

        for (const FDBToolRequest& request : requests()) {
            Timer tinspect("inspect", Log::debug<LibFdb5>());
            Timer tfdb("fdb.inspect", Log::debug<LibFdb5>());
            auto r = fdb.inspect(request.request());
            tfdb.stop();

            size_t count = 0;
            ListElement elem;
            while (r.next(elem)) ++count;
            eckit::Log::info() << "Count: " << count << std::endl;
            tinspect.stop();
        }
    };


    if (instances_ == 1) {
        workFn();
    } else {
        std::vector<std::future<void>> threads;
        for (long i = 0; i < instances_; ++i) {
            threads.emplace_back(std::async(std::launch::async, workFn));
        }
        for (auto& thread : threads) thread.get();
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace tools
} // namespace fdb5

int main(int argc, char **argv) {
    fdb5::tools::FDBInspectTest app(argc, argv);
    return app.start();
}

