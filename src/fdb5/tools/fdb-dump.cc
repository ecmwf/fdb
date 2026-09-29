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

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/tools/FDBVisitTool.h"

using namespace eckit;
using namespace eckit::option;

namespace fdb5 {
namespace tools {

//----------------------------------------------------------------------------------------------------------------------

class FDBDump : public FDBVisitTool {

public:  // methods

    FDBDump(int argc, char** argv) : FDBVisitTool(argc, argv), simple_(false) {

        options_.push_back(new SimpleOption<bool>("simple", "Dump one (simpler) record per line"));
    }

private:  // methods

    void execute(const CmdArgs& args) override;
    void init(const CmdArgs& args) override;

private:  // members

    bool simple_;
};

//----------------------------------------------------------------------------------------------------------------------

void FDBDump::init(const CmdArgs& args) {
    FDBVisitTool::init(args);
    simple_ = args.getBool("simple", false);
}


void FDBDump::execute(const CmdArgs& args) {

    FDB fdb(config(args));

    for (const FDBToolRequest& request : requests()) {

        auto dumpIterator = fdb.dump(request, simple_);

        size_t count = 0;
        DumpElement elem;
        while (dumpIterator.next(elem)) {
            Log::info() << elem << std::endl;
            count++;
        }

        if (count == 0 && fail()) {
            std::ostringstream ss;
            ss << "No FDB entries found for: " << request << std::endl;
            throw FDBToolException(ss.str());
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace tools
}  // namespace fdb5

int main(int argc, char** argv) {
    fdb5::tools::FDBDump app(argc, argv);
    return app.start();
}
