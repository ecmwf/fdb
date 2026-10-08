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
#include <memory>

#include "eckit/log/JSON.h"
#include "fdb5/LibFdb5.h"

#include "fdb5/toc/TocStats.h"

using namespace eckit;

namespace fdb5 {

::eckit::ClassSpec TocDbStats::classSpec_ = {
    &DbStatsContent::classSpec(),
    "TocDbStats",
};
::eckit::Reanimator<TocDbStats> TocDbStats::reanimator_;

::eckit::ClassSpec TocIndexStats::classSpec_ = {
    &IndexStatsContent::classSpec(),
    "TocIndexStats",
};
::eckit::Reanimator<TocIndexStats> TocIndexStats::reanimator_;

//----------------------------------------------------------------------------------------------------------------------

TocDbStats::TocDbStats() :
    dbCount_(0),
    tocRecordsCount_(0),
    tocFileSize_(0),
    schemaFileSize_(0),
    ownedFilesSize_(0),
    adoptedFilesSize_(0),
    indexFilesSize_(0),
    ownedFilesCount_(0),
    adoptedFilesCount_(0),
    indexFilesCount_(0) {}

TocDbStats::TocDbStats(Stream& s) {

    s >> dbCount_;
    s >> tocRecordsCount_;

    s >> tocFileSize_;
    s >> schemaFileSize_;
    s >> ownedFilesSize_;
    s >> adoptedFilesSize_;
    s >> indexFilesSize_;

    s >> ownedFilesCount_;
    s >> adoptedFilesCount_;
    s >> indexFilesCount_;
}

TocDbStats& TocDbStats::operator+=(const TocDbStats& rhs) {

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

void TocDbStats::add(const DbStatsContent& rhs) {
    const TocDbStats& stats = dynamic_cast<const TocDbStats&>(rhs);
    *this += stats;
}

void TocDbStats::report(std::ostream& out, const char* indent) const {

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
    reportBytes(out, "Total owned size", tocFileSize_ + schemaFileSize_ + indexFilesSize_ + ownedFilesSize_, indent);
    reportBytes(out, "Total size",
                tocFileSize_ + schemaFileSize_ + indexFilesSize_ + ownedFilesSize_ + adoptedFilesSize_, indent);
}

void TocDbStats::json(JSON& json) const {

    json << "databases" << dbCount_;
    json << "tocRecords" << tocRecordsCount_;
    json << "tocSize" << tocFileSize_;
    json << "schemaSize" << schemaFileSize_;

    json << "ownedDataFiles" << ownedFilesCount_;
    json << "ownedDataSize" << ownedFilesSize_;
    json << "adoptedDataFiles" << adoptedFilesCount_;
    json << "adoptedDataSize" << adoptedFilesSize_;
    json << "indexFiles" << indexFilesCount_;
    json << "indexSize" << indexFilesSize_;

    json << "totalOwnedSize" << tocFileSize_ + schemaFileSize_ + indexFilesSize_ + ownedFilesSize_;
    json << "totalSize" << tocFileSize_ + schemaFileSize_ + indexFilesSize_ + ownedFilesSize_ + adoptedFilesSize_;
}

void TocDbStats::encode(Stream& s) const {

    s << dbCount_;
    s << tocRecordsCount_;

    s << tocFileSize_;
    s << schemaFileSize_;
    s << ownedFilesSize_;
    s << adoptedFilesSize_;
    s << indexFilesSize_;

    s << ownedFilesCount_;
    s << adoptedFilesCount_;
    s << indexFilesCount_;
}

//----------------------------------------------------------------------------------------------------------------------

TocIndexStats::TocIndexStats() : fieldsCount_(0), duplicatesCount_(0), fieldsSize_(0), duplicatesSize_(0) {}

TocIndexStats::TocIndexStats(Stream& s) {
    s >> fieldsCount_;
    s >> duplicatesCount_;
    s >> fieldsSize_;
    s >> duplicatesSize_;
}


TocIndexStats& TocIndexStats::operator+=(const TocIndexStats& rhs) {
    fieldsCount_ += rhs.fieldsCount_;
    duplicatesCount_ += rhs.duplicatesCount_;
    fieldsSize_ += rhs.fieldsSize_;
    duplicatesSize_ += rhs.duplicatesSize_;

    return *this;
}

void TocIndexStats::add(const IndexStatsContent& rhs) {
    const TocIndexStats& stats = dynamic_cast<const TocIndexStats&>(rhs);
    *this += stats;
}

void TocIndexStats::report(std::ostream& out, const char* indent) const {
    reportCount(out, "Fields", fieldsCount_, indent);
    reportBytes(out, "Size of fields", fieldsSize_, indent);
    reportCount(out, "Duplicated fields ", duplicatesCount_, indent);
    reportBytes(out, "Size of duplicates", duplicatesSize_, indent);
    reportCount(out, "Reacheable fields ", fieldsCount_ - duplicatesCount_, indent);
    reportBytes(out, "Reachable size", fieldsSize_ - duplicatesSize_, indent);
}

void TocIndexStats::json(JSON& json) const {
    json << "fields" << fieldsCount_;
    json << "fieldsSize" << fieldsSize_;
    json << "duplicates" << duplicatesCount_;
    json << "duplicatesSize" << duplicatesSize_;
    json << "reachable" << fieldsCount_ - duplicatesCount_;
    json << "reachableSize" << fieldsSize_ - duplicatesSize_;
}

void TocIndexStats::encode(Stream& s) const {
    s << fieldsCount_;
    s << duplicatesCount_;
    s << fieldsSize_;
    s << duplicatesSize_;
}

//----------------------------------------------------------------------------------------------------------------------

TocDataStats::TocDataStats() {}

TocDataStats& TocDataStats::operator+=(const TocDataStats& rhs) {

    std::set<eckit::PathName> intersect;
    std::set_union(allDataFiles_.begin(), allDataFiles_.end(), rhs.allDataFiles_.begin(), rhs.allDataFiles_.end(),
                   std::insert_iterator<std::set<eckit::PathName>>(intersect, intersect.begin()));

    std::swap(allDataFiles_, intersect);

    intersect.clear();
    std::set_union(activeDataFiles_.begin(), activeDataFiles_.end(), rhs.activeDataFiles_.begin(),
                   rhs.activeDataFiles_.end(),
                   std::insert_iterator<std::set<eckit::PathName>>(intersect, intersect.begin()));

    std::swap(activeDataFiles_, intersect);

    for (const auto& [path, size] : rhs.dataUsage_) {
        dataUsage_[path] += size;
    }

    return *this;
}

void TocDataStats::add(const DataStatsContent& rhs) {
    const TocDataStats& stats = dynamic_cast<const TocDataStats&>(rhs);
    *this += stats;
}

void TocDataStats::report(std::ostream& out, const char* indent) const {
    NOTIMP;
}

//----------------------------------------------------------------------------------------------------------------------

TocStatsReportVisitor::TocStatsReportVisitor(const TocCatalogue& catalogue, bool includeReferenced) :
    directory_(catalogue.basePath()), includeReferencedNonOwnedData_(includeReferenced) {

    currentCatalogue_ = &catalogue;
    dbStats_ = catalogue.stats();
}

TocStatsReportVisitor::~TocStatsReportVisitor() {}

bool TocStatsReportVisitor::visitDatabase(const Catalogue& catalogue) {
    ASSERT(&catalogue == currentCatalogue_);
    return true;
}

EntryVisitor::IndexScopePtr TocStatsReportVisitor::visitIndex(const Index& index, OrderedParallelFor::Order& order) {

    auto& accumulator = perIndexAccumulators_.emplace_back(index);

    // Release the threads to start opening as many indexes in parallel as possible as
    // soon as possible
    order.release();

    return std::make_unique<TocIndexScope>(*currentCatalogue_, index, indexRule(index), accumulator);
}

void TocStatsReportVisitor::visitDatum(IndexScope& scope, const Field& field, const std::string& fieldFingerprint) {

    ASSERT(dynamic_cast<TocIndexScope*>(&scope));
    auto& tocScope = static_cast<TocIndexScope&>(scope);
    auto& accumulator = tocScope.accumulator();

    const auto& dataURI = field.location().uri();

    // Cache the current data path, as the URI --> PathName conversion becomes limiting
    // otherwise (profiled)

    bool dataPathConsidered = true;
    if (dataURI != tocScope.lastDataURI()) {
        dataPathConsidered = false;
        tocScope.lastDataURI(dataURI);
        tocScope.lastDataPath(dataURI.path());
    }
    const auto& dataPath = tocScope.lastDataPath();

    // Exclude non-owned data if relevant

    if (!includeReferencedNonOwnedData_) {
        const TocCatalogue* cat = dynamic_cast<const TocCatalogue*>(currentCatalogue_);

        if (!tocScope.indexParentPath().sameAs(cat->basePath())) {
            return;
        }
        if (!dataPath.dirName().sameAs(cat->basePath())) {
            return;
        }
    }

    // If this is a new data path, add it to the appropriate lists (if non-skipped)

    if (!dataPathConsidered) {
        auto it = std::find_if(accumulator.dataPaths.begin(), accumulator.dataPaths.end(),
                               [&](const DataFile& elem) { return elem.path == dataPath; });
        if (it == accumulator.dataPaths.end()) {
            if (dataPath.exists()) {
                accumulator.dataPaths.push_back(
                    {dataPath, dataPath.size(), !dataPath.dirName().sameAs(directory_), true});
            }
            else {
                accumulator.dataPaths.push_back({dataPath, 0, false, false});
            }
            it = std::prev(accumulator.dataPaths.end());
        }
        tocScope.lastDataPathIndex(it - accumulator.dataPaths.begin());
    }

    // Accumulate statistics

    Length len = field.location().length();
    accumulator.indexStats->addFieldsCount(1);
    accumulator.indexStats->addFieldsSize(len);

    // Add field-specific info to queue to be processed downstream

    std::string unique = scope.index().key().valuesToString() + "+" + fieldFingerprint;
    accumulator.fieldQueue.push_back({std::move(unique), tocScope.lastDataPathIndex(), len});
}

void TocStatsReportVisitor::catalogueComplete(const Catalogue& catalogue) {

    // Run through the stored per-database stats, and aggregate

    auto dbStats = std::make_unique<TocDbStats>();

    // Consume as we go, not clear at the end, so that we don't double memory footprint inserting
    // unique fingerprints into active_

    while (!perIndexAccumulators_.empty()) {
        auto acc = std::move(perIndexAccumulators_.front());
        perIndexAccumulators_.pop_front();

        for (const DataFile& dataFile : acc.dataPaths) {
            if (dataFile.exists && allDataFiles_.insert(dataFile.path).second) {
                if (dataFile.adopted) {
                    dbStats->adoptedFilesSize_ += dataFile.size;
                    dbStats->adoptedFilesCount_++;
                }
                else {
                    dbStats->ownedFilesSize_ += dataFile.size;
                    dbStats->ownedFilesCount_++;
                }
            }
        }

        // Indexes which are non-owned will contribute no fields as a result of the filter in
        // visitDatam. We exclude them here with the fieldsCount check.

        if (acc.indexStats->fieldsCount() > 0 && allIndexFiles_.insert(acc.indexPath()).second) {
            dbStats->indexFilesSize_ += acc.indexPathSize();
            dbStats->indexFilesCount_++;
        }

        // Now that all index processing is complete, we can confidently process the fields
        // in order and test for duplicates/masking

        ASSERT(indexStats_.emplace(acc.index, IndexStats(acc.indexStats.release())).second);

        for (FieldRecord& fieldRecord : acc.fieldQueue) {
            const eckit::PathName& dataPath = acc.dataPaths[fieldRecord.dataFile].path;

            if (active_.insert(std::move(fieldRecord.fingerprint)).second) {
                indexUsage_[acc.indexPath()]++;
                dataUsage_[dataPath]++;
            }
            else {
                indexStats_[acc.index].addDuplicatesCount(1);
                indexStats_[acc.index].addDuplicatesSize(fieldRecord.length);

                // Ensure these counts exist (as zero if otherwise unused).
                indexUsage_[acc.indexPath()];
                dataUsage_[dataPath];
            }
        }
    }

    ASSERT(perIndexAccumulators_.empty());
    dbStats_ += DbStats(dbStats.release());
}


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


}  // namespace fdb5
