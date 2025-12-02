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
#include "fdb5/database/DbStats.h"
#include "fdb5/database/IndexStats.h"
#include "fdb5/tools/FDBVisitTool.h"

#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"

using namespace eckit;
using namespace eckit::option;


namespace fdb5 {
namespace tools {

//----------------------------------------------------------------------------------------------------------------------

class FDBStats : public FDBVisitTool {

public:  // methods

    FDBStats(int argc, char** argv) : FDBVisitTool(argc, argv, "class,expver"), details_(false) {

        options_.push_back(new SimpleOption<bool>("details", "Print report for each database visited"));
    }

    ~FDBStats() override {}

private:  // methods

    void execute(const CmdArgs& args) override;
    void init(const CmdArgs& args) override;

private:  // members

    bool details_;
};

void FDBStats::init(const eckit::option::CmdArgs& args) {
    FDBVisitTool::init(args);
    details_ = args.getBool("details", false);
}

void FDBStats::execute(const CmdArgs& args) {

    FDB fdb(config(args));
    IndexStats totalIndexStats;
    DbStats totaldbStats;
    size_t count = 0;

    for (const FDBToolRequest& request : requests()) {

        auto statsIterator = fdb.stats(request);

        StatsElement elem;
        while (statsIterator.next(elem)) {

            if (details_) {
                Log::info() << std::endl;
                elem.indexStatistics.report(Log::info());
                elem.dbStatistics.report(Log::info());
                Log::info() << std::endl;
            }

            if (count == 0) {
                totalIndexStats = elem.indexStatistics;
                totaldbStats = elem.dbStatistics;
            }
            else {
                totalIndexStats += elem.indexStatistics;
                totaldbStats += elem.dbStatistics;
            }

            count++;
        }

        if (count == 0 && fail()) {
            std::stringstream ss;
            ss << "No FDB entries found for: " << request << std::endl;
            throw FDBToolException(ss.str());
        }
    }

    if (count > 0) {
        Log::info() << std::endl;
        Log::info() << "Summary:" << std::endl;
        Log::info() << "========" << std::endl;

        Log::info() << std::endl;
        Statistics::reportCount(Log::info(), "Number of databases", count);
        totalIndexStats.report(Log::info());
        totaldbStats.report(Log::info());
        Log::info() << std::endl;
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace tools
}  // namespace fdb5

int main(int argc, char** argv) {
    fdb5::tools::FDBStats app(argc, argv);
    return app.start();
}
