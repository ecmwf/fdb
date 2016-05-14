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
#include "fdb5/toc/TocHandler.h"
#include "fdb5/tools/FDBInspect.h"
#include "fdb5/database/Index.h"
#include "fdb5/toc/IndexStatistics.h"
#include "fdb5/toc/ReportVisitor.h"
#include "eckit/log/BigNum.h"

//----------------------------------------------------------------------------------------------------------------------

//----------------------------------------------------------------------------------------------------------------------

class FDBStat : public fdb5::FDBInspect {

  public: // methods

    FDBStat(int argc, char **argv) :
        fdb5::FDBInspect(argc, argv),
        count_(0),
        details_(false) {
        options_.push_back(new eckit::option::SimpleOption<bool>("details", "Print report for each database visited"));
    }

  private: // methods

    virtual void process(const eckit::PathName &, const eckit::option::CmdArgs &args);
    virtual void usage(const std::string &tool) const;
    virtual void init(const eckit::option::CmdArgs &args);
    virtual void finish(const eckit::option::CmdArgs &args);
    fdb5::IndexStatistics indexStats_;
    fdb5::DbStatistics dbStats_;

    size_t count_;
    bool details_;

};

void FDBStat::usage(const std::string &tool) const {

    eckit::Log::info() << std::endl << "Usage: " << tool << " [--details] [path1|request1] [path2|request2] ..." << std::endl;
    FDBInspect::usage(tool);
}

void FDBStat::init(const eckit::option::CmdArgs &args) {
    args.get("details", details_);
}

void FDBStat::process(const eckit::PathName &path, const eckit::option::CmdArgs &args) {

    eckit::Log::info() << "Scanning " << path << std::endl;

    fdb5::TocHandler handler(path);
    eckit::Log::info() << "Database key: " << handler.databaseKey() << ", owner: " << handler.dbOwner() << std::endl;

    fdb5::ReportVisitor visitor(path);
    // handler.visitor(visitor);

    std::vector<fdb5::Index *> indexes = handler.loadIndexes();


    for (std::vector<fdb5::Index *>::const_iterator i = indexes.begin(); i != indexes.end(); ++i) {
        (*i)->entries(visitor);
    }

    // if(details)
    // visitor.report(eckit::Log::info());

    handler.freeIndexes(indexes);

    if (details_) {
        eckit::Log::info() << std::endl;
        visitor.indexStatistics().report(eckit::Log::info());
        visitor.dbStatistics().report(eckit::Log::info());
        eckit::Log::info() << std::endl;
    }

    indexStats_ += visitor.indexStatistics();
    dbStats_ += visitor.dbStatistics();

    count_ ++;
}


void FDBStat::finish(const eckit::option::CmdArgs &args) {
    eckit::Log::info() << std::endl;
    eckit::Log::info() << "Summary:" << std::endl;
    eckit::Log::info() << "========" << std::endl;

    eckit::Log::info() << std::endl;
    fdb5::Statistics::reportCount(eckit::Log::info(), "Number of databases", count_);
    indexStats_.report(eckit::Log::info());
    dbStats_.report(eckit::Log::info());
    eckit::Log::info() << std::endl;

}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FDBStat app(argc, argv);
    return app.start();
}
