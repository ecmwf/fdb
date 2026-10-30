// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/tools/FDBVisitTool.h"

#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"


using namespace eckit;
using namespace eckit::option;

namespace fdb5::tools {

//----------------------------------------------------------------------------------------------------------------------

/// Purges duplicate entries from the database and removes associated data (if owned, not adopted)

class FDBPurge : public FDBVisitTool {

public:  // methods

    FDBPurge(int argc, char** argv) :
        FDBVisitTool(argc, argv, "class,expver,stream,date,time"), doit_(false), porcelain_(false) {

        options_.push_back(new SimpleOption<bool>("doit", "Delete the files (data and indexes)"));
        options_.push_back(new SimpleOption<bool>("porcelain", "List only the deleted files"));
        options_.push_back(new SimpleOption<long>("threads", "Number of threads to use for index reading (default 8)"));
    }

private:  // methods

    virtual void init(const CmdArgs& args);
    virtual void execute(const CmdArgs& args);
    virtual void finish(const CmdArgs& args);

    bool doit_;
    bool porcelain_;
    int threads_{8};
};


void FDBPurge::init(const CmdArgs& args) {
    FDBVisitTool::init(args);
    doit_ = args.getBool("doit", false);
    porcelain_ = args.getBool("porcelain", false);
    threads_ = args.getInt("threads", threads_);
}

void FDBPurge::execute(const CmdArgs& args) {

    LocalConfiguration userConfig;
    userConfig.set("readIndexThreads", threads_);
    FDB fdb(config(args, userConfig));

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

        if (count == 0 && failOnNoData()) {
            std::ostringstream ss;
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

}  // namespace fdb5::tools


int main(int argc, char** argv) {
    fdb5::tools::FDBPurge app(argc, argv);
    return app.start();
}
