// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/tools/FDBVisitTool.h"

using namespace eckit;
using namespace eckit::option;

namespace fdb5::tools {

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

        if (count == 0 && failOnNoData()) {
            std::ostringstream ss;
            ss << "No FDB entries found for: " << request << std::endl;
            throw FDBToolException(ss.str());
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::tools

int main(int argc, char** argv) {
    fdb5::tools::FDBDump app(argc, argv);
    return app.start();
}
