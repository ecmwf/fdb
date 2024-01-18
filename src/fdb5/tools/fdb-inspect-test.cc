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

using namespace eckit;
using namespace option;


namespace fdb5 {
namespace tools {

//----------------------------------------------------------------------------------------------------------------------

class FDBInspectTest : public FDBVisitTool {

public: // methods

    FDBInspectTest(int argc, char **argv) :
            FDBVisitTool(argc, argv, "class,expver") {}

private: // methods

    virtual void execute(const CmdArgs& args);
};

void FDBInspectTest::execute(const CmdArgs& args) {

    FDB fdb(config(args));

    for (const FDBToolRequest& request : requests()) {
        Timer tinspect("inspect");
        Timer tfdb("fdb.inspect");
        auto r = fdb.inspect(request.request());
        tfdb.stop();

        size_t count = 0;
        ListElement elem;
        while (r.next(elem)) {
            ++count;
//            Log::info() << elem << std::endl;
        }
        Log::info() << "Count: " << count << std::endl;
        tinspect.stop();
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace tools
} // namespace fdb5

int main(int argc, char **argv) {
    fdb5::tools::FDBInspectTest app(argc, argv);
    return app.start();
}

