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
using namespace eckit::option;

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

        options_.push_back(new SimpleOption<bool>("location", "Also print the location of each field"));
    }

  private: // methods

    virtual void execute(const CmdArgs& args);
    virtual void init(const CmdArgs &args);

    bool location_;
};

void FDBList::init(const CmdArgs& args) {

    FDBVisitTool::init(args);

    args.get("location", location_);
    // TODO: ignore-errors

}

void FDBList::execute(const CmdArgs& args) {

    FDB fdb;

    for (const FDBToolRequest& request : requests()) {

        auto listObject = fdb.list(request);

        size_t count = 0;
        ListElement elem;
        while (listObject.next(elem)) {
            elem.print(Log::info(), location_);
            Log::info() << std::endl;
            count++;
        }

        if (count == 0 && fail()) {
            std::stringstream ss;
            ss << "No FDB entries found for: " << request << std::endl;
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

