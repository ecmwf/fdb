/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/option/CmdArgs.h"
#include "eckit/log/BigNum.h"

#include "fdb5/database/Index.h"
#include "fdb5/tools/FDBInspect.h"

#include "fdb5/toc/TocHandler.h"
#include "fdb5/toc/TocStats.h"
#include "fdb5/toc/TocDBReader.h"

using namespace fdb5;

//----------------------------------------------------------------------------------------------------------------------


//----------------------------------------------------------------------------------------------------------------------

class FDBStats : public fdb5::FDBInspect {

  public: // methods

    FDBStats(int argc, char **argv) :
        fdb5::FDBInspect(argc, argv),
        totaldbStats_(new TocDbStats()),
        count_(0),
        details_(false) {
        options_.push_back(new eckit::option::SimpleOption<bool>("details", "Print report for each database visited"));
    }

    ~FDBStats() {}

  private: // methods

    virtual void process(const eckit::PathName &, const eckit::option::CmdArgs &args);
    virtual void usage(const std::string &tool) const;
    virtual void init(const eckit::option::CmdArgs &args);
    virtual void finish(const eckit::option::CmdArgs &args);


    IndexStats        totalIndexStats_;
    DbStats           totaldbStats_;

    size_t count_;
    bool details_;

};

void FDBStats::usage(const std::string &tool) const {

    eckit::Log::info() << std::endl << "Usage: " << tool << " [--details] [path1|request1] [path2|request2] ..." << std::endl;
    FDBInspect::usage(tool);
}

void FDBStats::init(const eckit::option::CmdArgs &args) {
    args.get("details", details_);
}

void FDBStats::process(const eckit::PathName &path, const eckit::option::CmdArgs &args) {

    eckit::Log::info() << "Scanning " << path << std::endl;

    eckit::ScopedPtr<DB> db(DBFactory::buildReader(path));
    db->open();

    eckit::Log::debug<LibFdb>() << "Database key: " << db->key() << ", owner: " << db->owner() << std::endl;

    eckit::ScopedPtr<StatsReportVisitor> visitor(db->statsReportVisitor());

    db->visitEntries(*visitor, true);

    IndexStats indexStats = visitor->indexStatistics();

    if (details_) {
        eckit::Log::info() << std::endl;
        indexStats.report(eckit::Log::info());
        visitor->dbStatistics().report(eckit::Log::info());
        eckit::Log::info() << std::endl;
    }

    if (count_ == 0) {
        totalIndexStats_ = indexStats;
        totaldbStats_ = visitor->dbStatistics();
    } else {
        totalIndexStats_ += indexStats;
        totaldbStats_ += visitor->dbStatistics();
    }

    count_ ++;
}


void FDBStats::finish(const eckit::option::CmdArgs &args) {
    eckit::Log::info() << std::endl;
    eckit::Log::info() << "Summary:" << std::endl;
    eckit::Log::info() << "========" << std::endl;

    eckit::Log::info() << std::endl;
    eckit::Statistics::reportCount(eckit::Log::info(), "Number of databases", count_);
    totalIndexStats_.report(eckit::Log::info());
    totaldbStats_.report(eckit::Log::info());
    eckit::Log::info() << std::endl;

}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FDBStats app(argc, argv);
    return app.start();
}
