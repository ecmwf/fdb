/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/tools/FDBVisitTool.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"


using namespace eckit;
using namespace eckit::option;

namespace fdb5 {
namespace tools {

//----------------------------------------------------------------------------------------------------------------------

/// Purges duplicate entries from the database and removes associated data (if owned, not adopted)

class FDBPurge : public FDBVisitTool {

public: // methods

    FDBPurge(int argc, char **argv) :
        FDBVisitTool(argc, argv, "class,expver,stream,date,time"),
        doit_(false) {

        options_.push_back(new SimpleOption<bool>("doit", "Delete the files (data and indexes)"));

    }

private: // methods

    virtual void init(const CmdArgs &args);
    virtual void execute(const CmdArgs& args);
    virtual void finish(const CmdArgs &args);

    bool doit_;
};


void FDBPurge::init(const CmdArgs& args) {
    FDBVisitTool::init(args);
    args.get("doit", doit_);
}

void FDBPurge::execute(const CmdArgs& args) {

    FDB fdb;

    for (const FDBToolRequest& request : requests()) {

        auto purgeIterator = fdb.purge(request, doit_);

        size_t count = 0;
        PurgeElement elem;
        while (purgeIterator.next(elem)) {
            Log::info() << elem << std::endl;
            count++;
        }

        if (count == 0 && fail()) {
            std::stringstream ss;
            ss << "No FDB entries found for: " << request << std::endl;
            throw FDBToolException(ss.str());
        }
    }
}


void FDBPurge::finish(const CmdArgs&) {

    if (!doit_) {
        Log::info() << std::endl
                    << "Rerun command with --doit flag to delete unused files"
                    << std::endl
                    << std::endl;
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace tools
} // namespace fdb5


int main(int argc, char **argv) {
    fdb5::tools::FDBPurge app(argc, argv);
    return app.start();
}
