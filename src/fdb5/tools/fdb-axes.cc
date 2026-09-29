/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/JSON.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/tools/FDBVisitTool.h"

using namespace eckit;
using namespace option;


namespace fdb5::tools {

//----------------------------------------------------------------------------------------------------------------------

class FDBAxisTest : public FDBVisitTool {

public:  // methods

    FDBAxisTest(int argc, char** argv) : FDBVisitTool(argc, argv, "class,expver"), level_(3), json_(false) {
        options_.push_back(
            new SimpleOption<long>("level", "Specify how many levels of the keys should be should be explored"));
        options_.push_back(new SimpleOption<bool>("json", "Output available fields in JSON form"));
    }

private:  // methods

    void execute(const CmdArgs& args) final;
    void init(const CmdArgs& args) final;

private:  // members

    int level_;
    bool json_;
};

void FDBAxisTest::init(const CmdArgs& args) {
    FDBVisitTool::init(args);
    json_ = args.getBool("json", json_);
    level_ = args.getInt("level", level_);
}

void FDBAxisTest::execute(const CmdArgs& args) {

    FDB fdb(config(args));

    for (const FDBToolRequest& request : requests()) {
        //        Timer taxes("axes");
        auto r = fdb.axes(request, level_);
        //        taxes.stop();

        if (json_) {
            JSON json(Log::info());
            json << r;
        }
        else {
            Log::info() << r;
        }
        Log::info() << std::endl;
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::tools

int main(int argc, char** argv) {
    fdb5::tools::FDBAxisTest app(argc, argv);
    return app.start();
}
