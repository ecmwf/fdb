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

    eckit::PathName destination_;
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
        destination_ = eckit::PathName(dest);
        if (!destination_.exists()) {
            std::stringstream ss;
            ss << "Destination root does not exist";
            throw UserError(ss.str(), Here());
        }
    }
}


void FDBMove::execute(const CmdArgs& args) {

    FDB fdb(config(args));

    
    for (const FDBToolRequest& request : requests("read")) {
        if (fdb.canMove(request)) {

            size_t count = 0;
            auto statusIterator = fdb.control(request, ControlAction::Disable, ControlIdentifier::Archive | ControlIdentifier::Wipe | ControlIdentifier::UniqueRoot);

            ControlElement elem;
            while (statusIterator.next(elem)) {

                ASSERT(elem.controlIdentifiers.has(ControlIdentifier::Archive));
                ASSERT(elem.controlIdentifiers.has(ControlIdentifier::Wipe));
                ASSERT(elem.controlIdentifiers.has(ControlIdentifier::UniqueRoot));

                fdb.move(elem, destination_);
                count++;
            }
            if (count == 0 && fail()) {
                std::stringstream ss;
                ss << "No FDB entries found for: " << request << std::endl;
                throw FDBToolException(ss.str());
            }
        } else {
            std::stringstream ss;
            ss << "Source DB cannot be moved" << std::endl;
            throw UserError(ss.str(), Here());
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace tools
} // namespace fdb5

int main(int argc, char **argv) {
    fdb5::tools::FDBMove app(argc, argv);
    return app.start();
}
