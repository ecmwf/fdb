/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Log.h"
#include "eckit/option/CmdArgs.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/tools/FDBVisitTool.h"

using namespace eckit::option;
using namespace eckit;


namespace fdb5 {
namespace tools {

//----------------------------------------------------------------------------------------------------------------------

class FDBWhere : public FDBVisitTool {
  public: // methods

    FDBWhere(int argc, char **argv) : FDBVisitTool(argc, argv) {}

  private: // methods

    virtual void execute(const CmdArgs& args) override;
};


void FDBWhere::execute(const CmdArgs&) {

    FDB fdb;

    for (const std::string& request : requests_) {
        FDBToolRequest tool_request(request, all_);
        auto whereIterator = fdb.where(tool_request);

        size_t count = 0;
        WhereElement elem;
        while (whereIterator.next(elem)) {
            Log::info() << elem << std::endl;
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
    fdb5::tools::FDBWhere app(argc, argv);
    return app.start();
}
