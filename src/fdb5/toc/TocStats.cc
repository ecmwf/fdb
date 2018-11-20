/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <algorithm>

#include "eckit/log/Log.h"
#include "fdb5/LibFdb.h"

#include "fdb5/toc/TocDB.h"
#include "fdb5/toc/TocStats.h"

using eckit::Log;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocDbStats::TocDbStats():
    dbCount_(0),
    tocRecordsCount_(0),
    tocFileSize_(0),
    schemaFileSize_(0),
    ownedFilesSize_(0),
    adoptedFilesSize_(0),
    indexFilesSize_(0),
    ownedFilesCount_(0),
    adoptedFilesCount_(0),
    indexFilesCount_(0)
{
}

TocDbStats& TocDbStats::operator+=(const TocDbStats &rhs) {

    dbCount_ += rhs.dbCount_;
    tocRecordsCount_ += rhs.tocRecordsCount_;
    tocFileSize_ += rhs.tocFileSize_;
    schemaFileSize_ += rhs.schemaFileSize_;
    ownedFilesSize_ += rhs.ownedFilesSize_;
    adoptedFilesSize_ += rhs.adoptedFilesSize_;
    indexFilesSize_ += rhs.indexFilesSize_;
    ownedFilesCount_ += rhs.ownedFilesCount_;
    adoptedFilesCount_ += rhs.adoptedFilesCount_;
    indexFilesCount_ += rhs.indexFilesCount_;

    return *this;
}

void TocDbStats::add(const DbStatsContent& rhs)
{
    const TocDbStats& stats = dynamic_cast<const TocDbStats&>(rhs);
    *this += stats;
}

void TocDbStats::report(std::ostream &out, const char *indent) const {

    reportCount(out, "Databases", dbCount_, indent);
    reportCount(out, "TOC records", tocRecordsCount_, indent);
    reportBytes(out, "Size of TOC files", tocFileSize_, indent);
    reportBytes(out, "Size of schemas files", schemaFileSize_, indent);
    reportCount(out, "TOC records", tocRecordsCount_, indent);

    reportCount(out, "Owned data files", ownedFilesCount_, indent);
    reportBytes(out, "Size of owned data files", ownedFilesSize_, indent);
    reportCount(out, "Adopted data files", adoptedFilesCount_, indent);

    reportBytes(out, "Size of adopted data files", adoptedFilesSize_, indent);
    reportCount(out, "Index files", indexFilesCount_, indent);

    reportBytes(out, "Size of index files", indexFilesSize_, indent);
    reportBytes(out, "Size of TOC files", tocFileSize_, indent);
    reportBytes(out, "Total owned size", tocFileSize_ + schemaFileSize_ +  indexFilesSize_ + ownedFilesSize_, indent);
    reportBytes(out, "Total size", tocFileSize_ + schemaFileSize_ +  indexFilesSize_ + ownedFilesSize_ + adoptedFilesSize_, indent);

}

//----------------------------------------------------------------------------------------------------------------------

TocIndexStats::TocIndexStats():
    fieldsCount_(0),
    duplicatesCount_(0),
    fieldsSize_(0),
    duplicatesSize_(0) {}


TocIndexStats &TocIndexStats::operator+=(const TocIndexStats &rhs) {
    fieldsCount_ += rhs.fieldsCount_;
    duplicatesCount_ += rhs.duplicatesCount_;
    fieldsSize_ += rhs.fieldsSize_;
    duplicatesSize_ += rhs.duplicatesSize_;

    return *this;
}

void TocIndexStats::add(const IndexStatsContent& rhs)
{
    const TocIndexStats& stats = dynamic_cast<const TocIndexStats&>(rhs);
    *this += stats;
}

void TocIndexStats::report(std::ostream &out, const char *indent) const {
    reportCount(out, "Fields", fieldsCount_, indent);
    reportBytes(out, "Size of fields", fieldsSize_, indent);
    reportCount(out, "Duplicated fields ", duplicatesCount_, indent);
    reportBytes(out, "Size of duplicates", duplicatesSize_, indent);
    reportCount(out, "Reacheable fields ", fieldsCount_ - duplicatesCount_, indent);
    reportBytes(out, "Reachable size", fieldsSize_ - duplicatesSize_, indent);
}

//----------------------------------------------------------------------------------------------------------------------

TocDataStats::TocDataStats() {
}

TocDataStats& TocDataStats::operator+=(const TocDataStats& rhs) {

    std::set<eckit::PathName> intersect;
    std::set_union(allDataFiles_.begin(),
                   allDataFiles_.end(),
                   rhs.allDataFiles_.begin(),
                   rhs.allDataFiles_.end(),
                   std::insert_iterator< std::set<eckit::PathName> >(intersect, intersect.begin()));

    std::swap(allDataFiles_, intersect);

    intersect.clear();
    std::set_union(activeDataFiles_.begin(),
                   activeDataFiles_.end(),
                   rhs.activeDataFiles_.begin(),
                   rhs.activeDataFiles_.end(),
                   std::insert_iterator< std::set<eckit::PathName> >(intersect, intersect.begin()));

    std::swap(activeDataFiles_, intersect);

    for(std::map<eckit::PathName, size_t>::const_iterator i = rhs.dataUsage_.begin(); i != rhs.dataUsage_.end(); ++i) {
        dataUsage_[i->first] += i->second;
    }

    return *this;
}

void TocDataStats::add(const DataStatsContent& rhs)
{
    const TocDataStats& stats = dynamic_cast<const TocDataStats&>(rhs);
    *this += stats;
}

void TocDataStats::report(std::ostream &out, const char *indent) const {
    NOTIMP;
}

//----------------------------------------------------------------------------------------------------------------------

TocStatsReportVisitor::TocStatsReportVisitor(const TocDB& db) :
    directory_(static_cast<const DB&>(db).basePath()) {

    currentDatabase_ = &db;
    dbStats_ = db.stats();
}

TocStatsReportVisitor::~TocStatsReportVisitor() {}

void TocStatsReportVisitor::visitDatabase(const DB &db) {
    ASSERT(&db == currentDatabase_);
}


void TocStatsReportVisitor::visitDatum(const Field& field, const std::string& fieldFingerprint) {

//    ASSERT(currIndex_ != 0);

    TocDbStats* dbStats = new TocDbStats();

    // If this index is not yet in the map, then create an entry

    std::map<Index, IndexStats>::iterator stats_it = indexStats_.find(*currentIndex_);

    if (stats_it == indexStats_.end()) {
        stats_it = indexStats_.insert(std::make_pair(*currentIndex_, IndexStats(new TocIndexStats()))).first;
    }

    IndexStats& stats(stats_it->second);

    eckit::Length len = field.location().length();

    stats.addFieldsCount(1);
    stats.addFieldsSize(len);

    const eckit::PathName& dataPath  = field.location().url();
    const eckit::PathName& indexPath = currentIndex_->location().url();

    if (dataPath != lastDataPath_) {

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

        lastDataPath_ = dataPath;
    }

    if (indexPath != lastIndexPath_) {

        if (allIndexFiles_.find(indexPath) == allIndexFiles_.end()) {
            dbStats->indexFilesSize_ += indexPath.size();
            allIndexFiles_.insert(indexPath);
            dbStats->indexFilesCount_++;
        }
        lastIndexPath_ = indexPath;
    }

    std::string unique = currentIndex_->key().valuesToString() + "+" + fieldFingerprint;

    if (active_.insert(unique).second) {
        indexUsage_[indexPath]++;
        dataUsage_[dataPath]++;
    } else {
        stats.addDuplicatesCount(1);
        stats.addDuplicatesSize(len);

        // Ensure these counts exist (as zero if otherwise unused).
        indexUsage_[indexPath];
        dataUsage_[dataPath];
    }

    dbStats_ += DbStats(dbStats); // append to the global dbStats
}

void TocStatsReportVisitor::databaseComplete(const DB &db) {}


DbStats TocStatsReportVisitor::dbStatistics() const {
    return dbStats_;
}

IndexStats TocStatsReportVisitor::indexStatistics() const {

    IndexStats total(new TocIndexStats());
    for (const auto& it : indexStats_) {
        total += it.second;
    }
    return total;
}

//----------------------------------------------------------------------------------------------------------------------


} // namespace fdb5
