/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/pmem/PMemStats.h"
#include "fdb5/pmem/PMemDBReader.h"

namespace fdb5 {
namespace pmem {

//----------------------------------------------------------------------------------------------------------------------


PMemDbStats::PMemDbStats():
    dataPoolsSize_(0),
    indexPoolsSize_(0),
    schemaSize_(0),
    dataPoolsCount_(0),
    indexPoolsCount_(0),
    indexesCount_(0) {}


PMemDbStats& PMemDbStats::operator+=(const PMemDbStats &rhs) {
    dataPoolsSize_ += rhs.dataPoolsSize_;
    indexPoolsSize_ += rhs.indexPoolsSize_;
    schemaSize_ += rhs.schemaSize_;
    dataPoolsCount_ += rhs.dataPoolsCount_;
    indexPoolsCount_ += rhs.indexPoolsCount_;
    indexesCount_ += rhs.indexesCount_;
    return *this;
}


void PMemDbStats::add(const DbStatsContent& rhs)
{
    const PMemDbStats& stats = dynamic_cast<const PMemDbStats&>(rhs);
    *this += stats;
}

void PMemDbStats::report(std::ostream &out, const char *indent) const {
    reportCount(out, "Data Pools ", dataPoolsCount_, indent);
    reportBytes(out, "Size of data pools", dataPoolsSize_, indent);
    reportCount(out, "Index Pools ", indexPoolsCount_, indent);
    reportBytes(out, "Size of index pools", indexPoolsSize_, indent);
    reportCount(out, "Indexes", indexesCount_, indent);
    reportBytes(out, "Size of schemas", schemaSize_, indent);
}


//----------------------------------------------------------------------------------------------------------------------


PMemIndexStats::PMemIndexStats():
    fieldsCount_(0),
    duplicatesCount_(0),
    fieldsSize_(0),
    duplicatesSize_(0) {}


PMemIndexStats &PMemIndexStats::operator+=(const PMemIndexStats &rhs) {
    fieldsCount_ += rhs.fieldsCount_;
    duplicatesCount_ += rhs.duplicatesCount_;
    fieldsSize_ += rhs.fieldsSize_;
    duplicatesSize_ += rhs.duplicatesSize_;

    return *this;
}

void PMemIndexStats::add(const IndexStatsContent& rhs)
{
    const PMemIndexStats& stats = dynamic_cast<const PMemIndexStats&>(rhs);
    *this += stats;
}

void PMemIndexStats::report(std::ostream &out, const char *indent) const {
    reportCount(out, "Fields", fieldsCount_, indent);
    reportBytes(out, "Size of fields", fieldsSize_, indent);
    reportCount(out, "Duplicated fields ", duplicatesCount_, indent);
    reportBytes(out, "Size of duplicates", duplicatesSize_, indent);
    reportCount(out, "Reacheable fields ", fieldsCount_ - duplicatesCount_, indent);
    reportBytes(out, "Reachable size", fieldsSize_ - duplicatesSize_, indent);
}


//----------------------------------------------------------------------------------------------------------------------


PMemDataStats::PMemDataStats() {
}


PMemDataStats &PMemDataStats::operator+=(const PMemDataStats &rhs) {

    NOTIMP;

    return *this;
}

void PMemDataStats::add(const DataStatsContent& rhs)
{
    const PMemDataStats& stats = dynamic_cast<const PMemDataStats&>(rhs);
    *this += stats;
}

void PMemDataStats::report(std::ostream &out, const char *indent) const {
    NOTIMP;
}


//----------------------------------------------------------------------------------------------------------------------


PMemStatsReportVisitor::PMemStatsReportVisitor(pmem::PMemDBReader& reader) :
//    directory_(reader.directory()),
    dbStats_(new PMemDbStats()),
    reader_(reader) {

    dbStats_ = static_cast<DB&>(reader_).statistics();
}

PMemStatsReportVisitor::~PMemStatsReportVisitor() {
}

void PMemStatsReportVisitor::visit(const Index &index,
                          const Field& field,
                          const std::string &indexFingerprint,
                          const std::string &fieldFingerprint) {

//    ASSERT(currIndex_ != 0);

    // If this index is not yet in the map, then create an entry

    std::map<Index, IndexStats>::iterator stats_it = indexStats_.find(index);

    if (stats_it == indexStats_.end()) {
        stats_it = indexStats_.insert(std::make_pair(index, IndexStats(new PMemIndexStats()))).first;
    }

    IndexStats& stats(stats_it->second);

    eckit::Length len = field.location().length();

    stats.addFieldsCount(1);
    stats.addFieldsSize(len);

    const eckit::PathName& dataPath  = field.location().url();
    const eckit::PathName& indexPath = index.location().url();

    // n.b. Unlike in the Toc case, we don't need to track the index and data pools here. They are stored and
    //      referenced centrally in the master pool, so the data about them is ALREADY located in the global
    //      dbStats!

    std::string unique = indexFingerprint + "+" + fieldFingerprint;

    if (active_.insert(unique).second) {
        indexUsage_[indexPath]++;
        dataUsage_[dataPath]++;
    } else {
        stats.addDuplicatesCount(1);
        stats.addDuplicatesSize(len);
    }
}

DbStats PMemStatsReportVisitor::dbStatistics() const {
    return dbStats_;
}

IndexStats PMemStatsReportVisitor::indexStatistics() const {

    IndexStats total(new PMemIndexStats());
    for (std::map<Index, IndexStats>::const_iterator i = indexStats_.begin(); i != indexStats_.end(); ++i) {
        total += i->second;
    }
    return total;
}



//----------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5
