/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/tools/FDBVisitTool.h"

#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"


using namespace eckit;
using namespace eckit::option;

namespace fdb5 {
namespace tools {

//----------------------------------------------------------------------------------------------------------------------

/// Purges duplicate entries from the database and removes associated data (if owned, not adopted)

class FDBPurge : public FDBVisitTool {

public:  // methods

    FDBPurge(int argc, char** argv) :
        FDBVisitTool(argc, argv, "class,expver,stream,date,time"),
        doit_(false),
        porcelain_(false),
        ignoreNoData_(false) {

        options_.push_back(new SimpleOption<bool>("doit", "Delete the files (data and indexes)"));
        options_.push_back(new SimpleOption<bool>("ignore-no-data", "No data available to delete is not an error"));
        options_.push_back(new SimpleOption<bool>("porcelain", "List only the deleted files"));
    }

private:  // methods

    virtual void init(const CmdArgs& args);
    virtual void execute(const CmdArgs& args);
    virtual void finish(const CmdArgs& args);

    bool doit_;
    bool porcelain_;
    bool ignoreNoData_;
};


void FDBPurge::init(const CmdArgs& args) {
    FDBVisitTool::init(args);
    doit_ = args.getBool("doit", false);
    porcelain_ = args.getBool("porcelain", false);
    ignoreNoData_ = args.getBool("ignore-no-data", false);
}

void FDBPurge::execute(const CmdArgs& args) {
    FDB fdb(config(args));
    for (const FDBToolRequest& request : requests()) {

        if (!porcelain_) {
            Log::info() << "Purging for request" << std::endl;
            request.print(Log::info());
            Log::info() << std::endl;
        }

        auto purgeIterator = fdb.purge(request, doit_, porcelain_);

        size_t count = 0;
        PurgeElement elem;
        while (purgeIterator.next(elem)) {
            Log::info() << elem << std::endl;
            count++;
        }

        if (count == 0 && fail() && !ignoreNoData_) {
            std::stringstream ss;
            ss << "No FDB entries found for: " << request << std::endl;
            throw FDBToolException(ss.str());
        }
    }
}


void FDBPurge::finish(const CmdArgs&) {

    if (!doit_ && !porcelain_) {
        Log::info() << std::endl << "Rerun command with --doit flag to delete unused files" << std::endl << std::endl;
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace tools
}  // namespace fdb5


int main(int argc, char** argv) {
    fdb5::tools::FDBPurge app(argc, argv);
    return app.start();
}
