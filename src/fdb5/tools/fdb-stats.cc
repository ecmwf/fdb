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

#include "eckit/log/JSON.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"

using namespace eckit;
using namespace eckit::option;


namespace fdb5 {
namespace tools {

//----------------------------------------------------------------------------------------------------------------------

class FDBStats : public FDBVisitTool {

public:  // methods

    FDBStats(int argc, char** argv) : FDBVisitTool(argc, argv, "class,expver"), details_(false), json_(false) {

        options_.push_back(new SimpleOption<bool>("details", "Print report for each database visited"));
        options_.push_back(new SimpleOption<bool>("json", "Output the statistics in JSON form"));
        options_.push_back(new SimpleOption<long>("threads", "Number of threads to use for index reading (default 8)"));
    }

    ~FDBStats() override {}

private:  // methods

    void execute(const CmdArgs& args) override;
    void init(const CmdArgs& args) override;

private:  // members

    bool details_;
    bool json_;
    int threads_{8};
};

void FDBStats::init(const eckit::option::CmdArgs& args) {
    FDBVisitTool::init(args);
    details_ = args.getBool("details", false);
    json_ = args.getBool("json", false);
    threads_ = args.getInt("threads", threads_);
}

void FDBStats::execute(const CmdArgs& args) {

    LocalConfiguration userConfig;
    userConfig.set("readIndexThreads", threads_);
    FDB fdb(config(args, userConfig));

    IndexStats totalIndexStats;
    DbStats totaldbStats;
    size_t count = 0;

    std::unique_ptr<JSON> json;
    if (json_) {
        json = std::make_unique<JSON>(Log::info());
        json->startObject();
        if (details_) {
            (*json) << "details";
            json->startList();
        }
    }

    for (const FDBToolRequest& request : requests()) {

        auto statsIterator = fdb.stats(request);

        StatsElement elem;
        while (statsIterator.next(elem)) {

            if (details_) {
                if (json) {
                    json->startObject();
                    elem.indexStatistics.json(*json);
                    elem.dbStatistics.json(*json);
                    json->endObject();
                }
                else {
                    Log::info() << std::endl;
                    elem.indexStatistics.report(Log::info());
                    elem.dbStatistics.report(Log::info());
                    Log::info() << std::endl;
                }
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

        if (count == 0 && failOnNoData()) {
            std::ostringstream ss;
            ss << "No FDB entries found for: " << request << std::endl;
            throw FDBToolException(ss.str());
        }
    }

    if (json) {
        if (details_) {
            json->endList();
        }
        if (count > 0) {
            totalIndexStats.json(*json);
            totaldbStats.json(*json);
        }
        else {
            (*json) << "databases" << count;
        }
        json->endObject();
        Log::info() << std::endl;
    }
    else if (count > 0) {
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
