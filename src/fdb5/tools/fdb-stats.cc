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

using namespace fdb5;

//----------------------------------------------------------------------------------------------------------------------

class ReportVisitor : public EntryVisitor {
public:

    ReportVisitor(const eckit::PathName &directory);
    ~ReportVisitor();

    TocIndexStats* indexStatistics() const;
    DbStats    dbStatistics() const;

private: // methods



    virtual void visit(const Index& index,
                       const Field& field,
                       const std::string &indexFingerprint,
                       const std::string &fieldFingerprint);


protected: // members

    eckit::PathName directory_;

    std::set<eckit::PathName> activeDataFiles_;
    std::set<eckit::PathName> allDataFiles_;
    std::set<eckit::PathName> allIndexFiles_;
    std::map<eckit::PathName, size_t> indexUsage_;
    std::map<eckit::PathName, size_t> dataUsage_;

    std::set<std::string> active_;

    std::map<Index, TocIndexStats> indexStats_;

    DbStats dbStats_;
};

ReportVisitor::ReportVisitor(const eckit::PathName& directory) :
    directory_(directory),
    dbStats_(new TocDbStats())
{

    TocHandler handler(directory_);
    dbStats_ = handler.stats();
}

ReportVisitor::~ReportVisitor() {
}

void ReportVisitor::visit(const Index &index,
                          const Field& field,
                          const std::string &indexFingerprint,
                          const std::string &fieldFingerprint) {


    TocDbStats* dbStats = new TocDbStats();

    TocIndexStats& stats = indexStats_[index];

    eckit::Length len = field.location().length();

    ++stats.fieldsCount_;
    stats.fieldsSize_ += len;

    const eckit::PathName& dataPath  = field.location().url();
    const eckit::PathName& indexPath = index.location().url();

    if (allDataFiles_.find(dataPath) == allDataFiles_.end()) {
        if (dataPath.dirName().sameAs(directory_)) {
            dbStats->ownedFilesSize_ += dataPath.size();
            dbStats->ownedFilesCount_++;

        } else {
            dbStats->adoptedFilesSize_ += dataPath.size();
            dbStats->adoptedFilesCount_++;

        }
        allDataFiles_.insert(dataPath);
    }

    if (allIndexFiles_.find(indexPath) == allIndexFiles_.end()) {
        dbStats->indexFilesSize_ += indexPath.size();
        allIndexFiles_.insert(indexPath);
        dbStats->indexFilesCount_++;
    }

    indexUsage_[indexPath]++;
    dataUsage_[dataPath]++;

    std::string unique = indexFingerprint + "+" + fieldFingerprint;

    if (active_.find(unique) == active_.end()) {
        active_.insert(unique);
        activeDataFiles_.insert(dataPath);
    } else {
        ++stats.duplicatesCount_;
        stats.duplicatesSize_ += len;
        indexUsage_[indexPath]--;
        dataUsage_[dataPath]--;
    }

    dbStats_ += DbStats(dbStats); // append to the global dbStats
}

DbStats ReportVisitor::dbStatistics() const {
    return dbStats_;
}

TocIndexStats* ReportVisitor::indexStatistics() const {
    TocIndexStats* total = new TocIndexStats();
    for (std::map<Index, TocIndexStats>::const_iterator i = indexStats_.begin(); i != indexStats_.end(); ++i) {
        *total += i->second;
    }
    return total;
}

//----------------------------------------------------------------------------------------------------------------------

class FDBStats : public fdb5::FDBInspect {

  public: // methods

    FDBStats(int argc, char **argv) :
        fdb5::FDBInspect(argc, argv),
        totalIndexStats_(new TocIndexStats()),
        totaldbStats_(new TocDbStats()),
        count_(0),
        details_(false) {
        options_.push_back(new eckit::option::SimpleOption<bool>("details", "Print report for each database visited"));
    }

    ~FDBStats() { delete totalIndexStats_; }

  private: // methods

    virtual void process(const eckit::PathName &, const eckit::option::CmdArgs &args);
    virtual void usage(const std::string &tool) const;
    virtual void init(const eckit::option::CmdArgs &args);
    virtual void finish(const eckit::option::CmdArgs &args);


    TocIndexStats*  totalIndexStats_;
    DbStats         totaldbStats_;

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

    fdb5::TocHandler handler(path);

    eckit::Log::debug<LibFdb>() << "Database key: " << handler.databaseKey() << ", owner: " << handler.dbOwner() << std::endl;

    ReportVisitor visitor(path);

    std::vector<fdb5::Index> indexes = handler.loadIndexes();

    for (std::vector<fdb5::Index>::const_iterator i = indexes.begin(); i != indexes.end(); ++i) {
        i->entries(visitor);
    }

    TocIndexStats* indexStats = visitor.indexStatistics();

    if (details_) {
        eckit::Log::info() << std::endl;
        indexStats->report(eckit::Log::info());
        visitor.dbStatistics().report(eckit::Log::info());
        eckit::Log::info() << std::endl;
    }

    totalIndexStats_->add(*indexStats);
    totaldbStats_ += visitor.dbStatistics();

    count_ ++;

    delete indexStats;
}


void FDBStats::finish(const eckit::option::CmdArgs &args) {
    eckit::Log::info() << std::endl;
    eckit::Log::info() << "Summary:" << std::endl;
    eckit::Log::info() << "========" << std::endl;

    eckit::Log::info() << std::endl;
    eckit::Statistics::reportCount(eckit::Log::info(), "Number of databases", count_);
    totalIndexStats_->report(eckit::Log::info());
    totaldbStats_.report(eckit::Log::info());
    eckit::Log::info() << std::endl;

}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FDBStats app(argc, argv);
    return app.start();
}
