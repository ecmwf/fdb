/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_TocDbStats_H
#define fdb5_TocDbStats_H

#include <iosfwd>
#include <map>
#include <set>

#include "eckit/filesystem/PathName.h"

#include "fdb5/database/DataStats.h"
#include "fdb5/database/DbStats.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/IndexStats.h"
#include "fdb5/database/StatsReportVisitor.h"
#include "fdb5/toc/TocCatalogueReader.h"

#include <unordered_map>
#include <unordered_set>

namespace fdb5 {


//----------------------------------------------------------------------------------------------------------------------


class TocDbStats : public DbStatsContent {
public:

    TocDbStats();
    TocDbStats(eckit::Stream& s);

    static DbStats make() { return DbStats(new TocDbStats()); }

    size_t dbCount_;
    size_t tocRecordsCount_;

    unsigned long long tocFileSize_;
    unsigned long long schemaFileSize_;
    unsigned long long ownedFilesSize_;
    unsigned long long adoptedFilesSize_;
    unsigned long long indexFilesSize_;

    size_t ownedFilesCount_;
    size_t adoptedFilesCount_;
    size_t indexFilesCount_;

    TocDbStats& operator+=(const TocDbStats& rhs);

    void add(const DbStatsContent&) override;

    void report(std::ostream& out, const char* indent) const override;

public:  // For Streamable

    static const eckit::ClassSpec& classSpec() { return classSpec_; }

protected:  // For Streamable

    void encode(eckit::Stream&) const override;
    const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }

    static eckit::ClassSpec classSpec_;
    static eckit::Reanimator<TocDbStats> reanimator_;
};


//----------------------------------------------------------------------------------------------------------------------


class TocIndexStats : public IndexStatsContent {
public:

    TocIndexStats();
    TocIndexStats(eckit::Stream& s);

    size_t fieldsCount_;
    size_t duplicatesCount_;

    unsigned long long fieldsSize_;
    unsigned long long duplicatesSize_;

    size_t fieldsCount() const override { return fieldsCount_; }
    size_t duplicatesCount() const override { return duplicatesCount_; }

    size_t fieldsSize() const override { return fieldsSize_; }
    size_t duplicatesSize() const override { return duplicatesSize_; }

    size_t addFieldsCount(size_t i) override {
        fieldsCount_ += i;
        return fieldsCount_;
    }
    size_t addDuplicatesCount(size_t i) override {
        duplicatesCount_ += i;
        return duplicatesCount_;
    }

    size_t addFieldsSize(size_t i) override {
        fieldsSize_ += i;
        return fieldsSize_;
    }
    size_t addDuplicatesSize(size_t i) override {
        duplicatesSize_ += i;
        return duplicatesSize_;
    }

    TocIndexStats& operator+=(const TocIndexStats& rhs);

    void add(const IndexStatsContent&) override;

    void report(std::ostream& out, const char* indent = "") const override;

public:  // For Streamable

    static const eckit::ClassSpec& classSpec() { return classSpec_; }

protected:  // For Streamable

    void encode(eckit::Stream&) const override;
    const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }

    static eckit::ClassSpec classSpec_;
    static eckit::Reanimator<TocIndexStats> reanimator_;
};


//----------------------------------------------------------------------------------------------------------------------


class TocDataStats : public DataStatsContent {
public:

    TocDataStats();

    std::set<eckit::PathName> allDataFiles_;
    std::set<eckit::PathName> activeDataFiles_;
    std::map<eckit::PathName, size_t> dataUsage_;

    TocDataStats& operator+=(const TocDataStats& rhs);

    virtual void add(const DataStatsContent&);

    virtual void report(std::ostream& out, const char* indent) const;
};


//----------------------------------------------------------------------------------------------------------------------


class TocStatsReportVisitor : public virtual StatsReportVisitor {
public:

    TocStatsReportVisitor(const TocCatalogue& catalogue, bool includeReferenced = true);
    ~TocStatsReportVisitor() override;

    IndexStats indexStatistics() const override;
    DbStats dbStatistics() const override;

private:  // methods

    bool visitDatabase(const Catalogue& catalogue) override;
    void visitDatum(const Field& field, const std::string& keyFingerprint) override;

    void visitDatum(const Field& /*field*/, const Key& /*datumKey*/) override { NOTIMP; }

    // This visitor is only legit for one DB - so don't reset database
    void catalogueComplete(const Catalogue& catalogue) override;


protected:  // members

    eckit::PathName directory_;

    // Use of unordered_set/unordered_map is significant here from a performance perspective

    std::unordered_set<std::string> allDataFiles_;
    std::unordered_set<std::string> allIndexFiles_;

    std::unordered_map<std::string, size_t> indexUsage_;
    std::unordered_map<std::string, size_t> dataUsage_;

    std::unordered_set<std::string> active_;

    std::map<Index, IndexStats> indexStats_;

    DbStats dbStats_;

    eckit::PathName lastDataPath_;
    eckit::PathName lastIndexPath_;

    // Where data has been adopted/fdb-mounted, should it be included in the stats?
    bool includeReferencedNonOwnedData_;
};


//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif
