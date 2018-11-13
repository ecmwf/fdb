/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/memory/ScopedPtr.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/config/Resource.h"
#include "eckit/option/SimpleOption.h"
#include "eckit/option/VectorOption.h"
#include "eckit/option/CmdArgs.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/database/DB.h"
#include "fdb5/database/Index.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/tools/FDBVisitTool.h"

using namespace eckit;

/*
 * This is a test case
 *
 * TODO: Generalise to more cases
 *
 */

namespace fdb5 {
namespace tools {

//----------------------------------------------------------------------------------------------------------------------

class FDBList : public FDBVisitTool {

  public: // methods

    FDBList(int argc, char **argv) :
        FDBVisitTool(argc, argv),
        location_(false) {

        options_.push_back(new eckit::option::SimpleOption<bool>("location", "Also print the location of each field"));
    }

  private: // methods

    virtual void execute(const eckit::option::CmdArgs& args);
    // virtual int minimumPositionalArguments() const { return 1; }
    virtual void init(const eckit::option::CmdArgs &args);

    bool location_;
};

void FDBList::init(const option::CmdArgs& args) {

    FDBVisitTool::init(args);

    args.get("location", location_);
    // TODO: ignore-errors

}

void FDBList::execute(const option::CmdArgs& args) {

    FDB fdb;

    for (const std::string& request : requests_) {

        FDBToolRequest tool_request(request, all_);
        auto listObject = fdb.list(tool_request);

        size_t count = 0;
        ListElement elem;
        while (listObject.next(elem)) {
            elem.print(Log::info(), location_);
            Log::info() << std::endl;
            count++;
        }

        if (count == 0 && fail_) {
            std::stringstream ss;
            ss << "No FDB entries found for: " << tool_request << std::endl;
            throw FDBToolException(ss.str());
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace tools
} // namespace fdb5

int main(int argc, char **argv) {
    fdb5::tools::FDBList app(argc, argv);
    return app.start();
}

