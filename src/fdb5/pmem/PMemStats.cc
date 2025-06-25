/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#include "fdb5/pmem/PMemStats.h"

#include "fdb5/pmem/PMemDB.h"

namespace fdb5 {
namespace pmem {

::eckit::ClassSpec PMemDbStats::classSpec_ = {
    &FieldLocation::classSpec(),
    "PMemDbStats",
};
::eckit::Reanimator<PMemDbStats> PMemDbStats::reanimator_;

::eckit::ClassSpec PMemIndexStats::classSpec_ = {
    &FieldLocation::classSpec(),
    "PMemIndexStats",
};
::eckit::Reanimator<PMemIndexStats> PMemIndexStats::reanimator_;

//----------------------------------------------------------------------------------------------------------------------


PMemDbStats::PMemDbStats() :
    dataPoolsSize_(0), indexPoolsSize_(0), schemaSize_(0), dataPoolsCount_(0), indexPoolsCount_(0), indexesCount_(0) {}

PMemDbStats::PMemDbStats(eckit::Stream& s) {
    s >> dataPoolsSize_;
    s >> indexPoolsSize_;
    s >> schemaSize_;

    s >> dataPoolsCount_;
    s >> indexPoolsCount_;
    s >> indexesCount_;
}


PMemDbStats& PMemDbStats::operator+=(const PMemDbStats& rhs) {
    dataPoolsSize_ += rhs.dataPoolsSize_;
    indexPoolsSize_ += rhs.indexPoolsSize_;
    schemaSize_ += rhs.schemaSize_;
    dataPoolsCount_ += rhs.dataPoolsCount_;
    indexPoolsCount_ += rhs.indexPoolsCount_;
    indexesCount_ += rhs.indexesCount_;
    return *this;
}


void PMemDbStats::add(const DbStatsContent& rhs) {
    const PMemDbStats& stats = dynamic_cast<const PMemDbStats&>(rhs);
    *this += stats;
}

void PMemDbStats::report(std::ostream& out, const char* indent) const {
    reportCount(out, "Data Pools ", dataPoolsCount_, indent);
    reportBytes(out, "Size of data pools", dataPoolsSize_, indent);
    reportCount(out, "Index Pools ", indexPoolsCount_, indent);
    reportBytes(out, "Size of index pools", indexPoolsSize_, indent);
    reportCount(out, "Indexes", indexesCount_, indent);
    reportBytes(out, "Size of schemas", schemaSize_, indent);
}

void PMemDbStats::encode(eckit::Stream& s) const {
    s << dataPoolsSize_;
    s << indexPoolsSize_;
    s << schemaSize_;

    s << dataPoolsCount_;
    s << indexPoolsCount_;
    s << indexesCount_;
}


//----------------------------------------------------------------------------------------------------------------------


PMemIndexStats::PMemIndexStats() : fieldsCount_(0), duplicatesCount_(0), fieldsSize_(0), duplicatesSize_(0) {}

PMemIndexStats::PMemIndexStats(eckit::Stream& s) {
    s >> fieldsCount_;
    s >> duplicatesCount_;
    s >> fieldsSize_;
    s >> duplicatesSize_;
}


PMemIndexStats& PMemIndexStats::operator+=(const PMemIndexStats& rhs) {
    fieldsCount_ += rhs.fieldsCount_;
    duplicatesCount_ += rhs.duplicatesCount_;
    fieldsSize_ += rhs.fieldsSize_;
    duplicatesSize_ += rhs.duplicatesSize_;

    return *this;
}

void PMemIndexStats::add(const IndexStatsContent& rhs) {
    const PMemIndexStats& stats = dynamic_cast<const PMemIndexStats&>(rhs);
    *this += stats;
}

void PMemIndexStats::report(std::ostream& out, const char* indent) const {
    reportCount(out, "Fields", fieldsCount_, indent);
    reportBytes(out, "Size of fields", fieldsSize_, indent);
    reportCount(out, "Duplicated fields ", duplicatesCount_, indent);
    reportBytes(out, "Size of duplicates", duplicatesSize_, indent);
    reportCount(out, "Reacheable fields ", fieldsCount_ - duplicatesCount_, indent);
    reportBytes(out, "Reachable size", fieldsSize_ - duplicatesSize_, indent);
}

void PMemIndexStats::encode(eckit::Stream& s) const {
    s << fieldsCount_;
    s << duplicatesCount_;
    s << fieldsSize_;
    s << duplicatesSize_;
}


//----------------------------------------------------------------------------------------------------------------------


PMemDataStats::PMemDataStats() {}


PMemDataStats& PMemDataStats::operator+=(const PMemDataStats& rhs) {

    NOTIMP;

    return *this;
}

void PMemDataStats::add(const DataStatsContent& rhs) {
    const PMemDataStats& stats = dynamic_cast<const PMemDataStats&>(rhs);
    *this += stats;
}

void PMemDataStats::report(std::ostream& out, const char* indent) const {
    NOTIMP;
}


//----------------------------------------------------------------------------------------------------------------------


PMemStatsReportVisitor::PMemStatsReportVisitor(const PMemDB& db) :
    //    directory_(reader.directory()),
    dbStats_(new PMemDbStats()) {

    currentDatabase_ = &db;
    dbStats_         = currentDatabase_->statistics();
}

PMemStatsReportVisitor::~PMemStatsReportVisitor() {}

bool PMemStatsReportVisitor::visitDatabase(const DB& db) {
    ASSERT(&db == currentDatabase_);
    return true;
}

void PMemStatsReportVisitor::visitDatum(const Field& field, const std::string& fieldFingerprint) {

    //    ASSERT(currIndex_ != 0);

    // If this index is not yet in the map, then create an entry

    std::map<Index, IndexStats>::iterator stats_it = indexStats_.find(*currentIndex_);

    if (stats_it == indexStats_.end()) {
        stats_it = indexStats_.insert(std::make_pair(*currentIndex_, IndexStats(new PMemIndexStats()))).first;
    }

    IndexStats& stats(stats_it->second);

    eckit::Length len = field.location().length();

    stats.addFieldsCount(1);
    stats.addFieldsSize(len);

    const eckit::PathName& dataPath  = field.location().url();
    const eckit::PathName& indexPath = currentIndex_->location().url();

    // n.b. Unlike in the Toc case, we don't need to track the index and data pools here. They are stored and
    //      referenced centrally in the master pool, so the data about them is ALREADY located in the global
    //      dbStats!

    std::string unique = currentIndex_->key().valuesToString() + "+" + fieldFingerprint;

    if (active_.insert(unique).second) {
        indexUsage_[indexPath]++;
        dataUsage_[dataPath]++;
    }
    else {
        stats.addDuplicatesCount(1);
        stats.addDuplicatesSize(len);
    }
}

void PMemStatsReportVisitor::databaseComplete(const DB& db) {}

DbStats PMemStatsReportVisitor::dbStatistics() const {
    return dbStats_;
}

IndexStats PMemStatsReportVisitor::indexStatistics() const {

    IndexStats total(new PMemIndexStats());
    for (const auto& it : indexStats_) {
        total += it.second;
    }
    return total;
}


//----------------------------------------------------------------------------------------------------------------------

}  // namespace pmem
}  // namespace fdb5
