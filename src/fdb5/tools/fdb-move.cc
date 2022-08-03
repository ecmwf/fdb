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

#include "fdb5/tools/FDBVisitTool.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/LibFdb5.h"

using namespace eckit::option;
using namespace eckit;

namespace fdb5 {
namespace tools {

//----------------------------------------------------------------------------------------------------------------------

class FDBMove : public FDBVisitTool {
public: // methods

    FDBMove(int argc, char **argv);
    ~FDBMove() override;

private: // methods

    void execute(const CmdArgs& args) override;
    void init(const CmdArgs &args) override;

private: // members

    eckit::URI destination_;
};

FDBMove::FDBMove(int argc, char **argv) :
    FDBVisitTool(argc, argv, "class,expver,stream,date,time"),
    destination_("") {

    options_.push_back(new SimpleOption<std::string>("dest", "Destination root"));
}

FDBMove::~FDBMove() {}


void FDBMove::init(const CmdArgs& args) {
    FDBVisitTool::init(args);

    std::string dest = args.getString("dest");

    if (dest.empty()) {
        std::stringstream ss;
        ss << "No destination root specified.";
        throw UserError(ss.str(), Here());
    } else {
        destination_ = eckit::URI(dest);
    }
}


void FDBMove::execute(const CmdArgs& args) {

    FDB fdb(config(args));

    size_t count = 0;
    for (const FDBToolRequest& request : requests("read")) {
        fdb.move(request, destination_);
        count++;
    }

    if (count == 0 && fail()) {
        std::stringstream ss;
        ss << "No FDB entries found" << std::endl;
        throw FDBToolException(ss.str());
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace tools
} // namespace fdb5

int main(int argc, char **argv) {
    fdb5::tools::FDBMove app(argc, argv);
    return app.start();
}
