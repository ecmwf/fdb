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
 * This software was developed as part of the Horizon Europe programme funded project OpenCUBE
 * (Grant agreement: 101092984) horizon-opencube.eu
 */

#include "fdb5/fam/FamStats.h"

#include "fdb5/database/Catalogue.h"
#include "fdb5/database/DbStats.h"
#include "fdb5/database/EntryVisitMechanism.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/IndexStats.h"
#include "fdb5/database/Key.h"
#include "fdb5/fam/FamCatalogue.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/serialisation/Reanimator.h"
#include "eckit/serialisation/Stream.h"

#include <cstddef>
#include <ostream>
#include <string>
#include <utility>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

::eckit::ClassSpec FamDbStats::classSpec_ = {&DbStatsContent::classSpec(), "FamDbStats"};
::eckit::Reanimator<FamDbStats> FamDbStats::reanimator_;

FamDbStats::FamDbStats(eckit::Stream& stream) {
    stream >> dbCount_;
    stream >> indexCount_;
    stream >> dataObjectCount_;
    stream >> dataObjectSize_;
    stream >> dataReachableSize_;
}

FamDbStats& FamDbStats::operator+=(const FamDbStats& rhs) {
    dbCount_ += rhs.dbCount_;
    indexCount_ += rhs.indexCount_;
    dataObjectCount_ += rhs.dataObjectCount_;
    dataObjectSize_ += rhs.dataObjectSize_;
    dataReachableSize_ += rhs.dataReachableSize_;
    return *this;
}

void FamDbStats::add(const DbStatsContent& rhs) {
    *this += dynamic_cast<const FamDbStats&>(rhs);
}

void FamDbStats::report(std::ostream& out, const char* indent) const {
    reportCount(out, "Databases", dbCount_, indent);
    reportCount(out, "Indexes", indexCount_, indent);
    reportCount(out, "Data objects", dataObjectCount_, indent);
    reportBytes(out, "Total data size", dataObjectSize_, indent);
    reportBytes(out, "Reachable data size", dataReachableSize_, indent);
}

void FamDbStats::encode(eckit::Stream& stream) const {
    stream << dbCount_;
    stream << indexCount_;
    stream << dataObjectCount_;
    stream << dataObjectSize_;
    stream << dataReachableSize_;
}

//----------------------------------------------------------------------------------------------------------------------

::eckit::ClassSpec FamIndexStats::classSpec_ = {&IndexStatsContent::classSpec(), "FamIndexStats"};
::eckit::Reanimator<FamIndexStats> FamIndexStats::reanimator_;

FamIndexStats::FamIndexStats(eckit::Stream& stream) {
    stream >> fieldsCount_;
    stream >> duplicatesCount_;
    stream >> fieldsSize_;
    stream >> duplicatesSize_;
}

FamIndexStats& FamIndexStats::operator+=(const FamIndexStats& rhs) {
    fieldsCount_ += rhs.fieldsCount_;
    duplicatesCount_ += rhs.duplicatesCount_;
    fieldsSize_ += rhs.fieldsSize_;
    duplicatesSize_ += rhs.duplicatesSize_;
    return *this;
}

void FamIndexStats::add(const IndexStatsContent& rhs) {
    *this += dynamic_cast<const FamIndexStats&>(rhs);
}

void FamIndexStats::report(std::ostream& out, const char* indent) const {
    reportCount(out, "Fields", fieldsCount_, indent);
    reportBytes(out, "Size of fields", fieldsSize_, indent);
    reportCount(out, "Duplicated fields", duplicatesCount_, indent);
    reportBytes(out, "Size of duplicates", duplicatesSize_, indent);
    reportCount(out, "Reachable fields", fieldsCount_ - duplicatesCount_, indent);
    reportBytes(out, "Reachable size", fieldsSize_ - duplicatesSize_, indent);
}

void FamIndexStats::encode(eckit::Stream& stream) const {
    stream << fieldsCount_;
    stream << duplicatesCount_;
    stream << fieldsSize_;
    stream << duplicatesSize_;
}

//----------------------------------------------------------------------------------------------------------------------

FamStatsReportVisitor::FamStatsReportVisitor(const FamCatalogue& catalogue) {
    currentCatalogue_ = &catalogue;
}

FamStatsReportVisitor::~FamStatsReportVisitor() = default;

bool FamStatsReportVisitor::visitDatabase(const Catalogue& catalogue) {
    ASSERT(&catalogue == currentCatalogue_);
    return true;
}

bool FamStatsReportVisitor::visitIndex(const Index& index) {
    // Count every visited index (including those with no fields), then explore its entries.
    const bool explore = EntryVisitor::visitIndex(index);
    indexObjects_.insert(index.location().uri().asRawString());
    return explore;
}

void FamStatsReportVisitor::visitDatum(const Field& field, const std::string& key_fingerprint) {
    ASSERT(currentIndex_);

    auto stats_it = indexStats_.find(*currentIndex_);
    if (stats_it == indexStats_.end()) {
        stats_it = indexStats_.emplace(*currentIndex_, IndexStats(new FamIndexStats())).first;
    }
    IndexStats& stats = stats_it->second;

    const auto length = static_cast<size_t>(field.location().length());
    const auto data_uri = field.location().uri().asRawString();

    stats.addFieldsCount(1);
    stats.addFieldsSize(length);

    dataSize_ += length;

    const auto unique = currentIndex_->key().valuesToString() + "+" + key_fingerprint;
    if (seen_.insert(unique).second) {
        dataUsage_[data_uri] += 1;
        reachableSize_ += length;
    }
    else {
        stats.addDuplicatesCount(1);
        stats.addDuplicatesSize(length);
        dataUsage_[data_uri];
    }
}

void FamStatsReportVisitor::visitDatum(const Field& /*field*/, const Key& /*datumKey*/) {
    NOTIMP;
}

void FamStatsReportVisitor::catalogueComplete(const Catalogue& /*catalogue*/) {}

IndexStats FamStatsReportVisitor::indexStatistics() const {
    IndexStats total(new FamIndexStats());
    for (const auto& [index, stats] : indexStats_) {
        total += stats;
    }
    return total;
}

DbStats FamStatsReportVisitor::dbStatistics() const {
    auto* stats = new FamDbStats();
    stats->dbCount_ = 1;
    stats->indexCount_ = indexObjects_.size();
    stats->dataObjectCount_ = dataUsage_.size();
    stats->dataObjectSize_ = dataSize_;
    stats->dataReachableSize_ = reachableSize_;
    return {stats};
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
