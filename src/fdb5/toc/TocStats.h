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

#include <set>
#include <map>
#include <iosfwd>

#include "eckit/filesystem/PathName.h"

#include "fdb5/database/DbStats.h"
#include "fdb5/database/IndexStats.h"
#include "fdb5/database/DataStats.h"
#include "fdb5/database/StatsReportVisitor.h"
#include "fdb5/database/Index.h"

#include <unordered_set>
#include <unordered_map>

namespace fdb5 {

class TocDB;

//----------------------------------------------------------------------------------------------------------------------


class TocDbStats : public DbStatsContent {
public:

    TocDbStats();

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

    TocDbStats& operator+= (const TocDbStats& rhs);

    virtual void add(const DbStatsContent&);

    virtual void report(std::ostream &out, const char* indent) const;
};


//----------------------------------------------------------------------------------------------------------------------


class TocIndexStats : public IndexStatsContent {
public:

    TocIndexStats();

    size_t fieldsCount_;
    size_t duplicatesCount_;

    unsigned long long fieldsSize_;
    unsigned long long duplicatesSize_;

    virtual size_t fieldsCount() const { return fieldsCount_; }
    virtual size_t duplicatesCount() const { return duplicatesCount_; }

    virtual size_t fieldsSize() const { return fieldsSize_; }
    virtual size_t duplicatesSize() const { return duplicatesSize_; }

    virtual size_t addFieldsCount(size_t i) { fieldsCount_ += i; return fieldsCount_; }
    virtual size_t addDuplicatesCount(size_t i) { duplicatesCount_ += i; return duplicatesCount_; }

    virtual size_t addFieldsSize(size_t i) { fieldsSize_ += i; return fieldsSize_; }
    virtual size_t addDuplicatesSize(size_t i) { duplicatesSize_ += i; return duplicatesSize_; }

    TocIndexStats& operator+= (const TocIndexStats& rhs);

    virtual void add(const IndexStatsContent&);

    virtual void report(std::ostream &out, const char* indent = "") const;
};


//----------------------------------------------------------------------------------------------------------------------


class TocDataStats : public DataStatsContent {
public:

    TocDataStats();

    std::set<eckit::PathName> allDataFiles_;
    std::set<eckit::PathName> activeDataFiles_;
    std::map<eckit::PathName, size_t> dataUsage_;

    TocDataStats& operator+= (const TocDataStats& rhs);

    virtual void add(const DataStatsContent&);

    virtual void report(std::ostream &out, const char* indent) const;
};


//----------------------------------------------------------------------------------------------------------------------


class TocStatsReportVisitor : public virtual StatsReportVisitor {
public:

    TocStatsReportVisitor(const TocDB& reader);
    virtual ~TocStatsReportVisitor() override;

    IndexStats indexStatistics() const override;
    DbStats    dbStatistics() const override;

private: // methods

    void visitDatabase(const DB& db) override;
    void visitDatum(const Field& field, const std::string& keyFingerprint) override;
    void visitDatum(const Field& field, const Key& key) override { NOTIMP; }

    // This visitor is only legit for one DB - so don't reset database
    void databaseComplete(const DB& db) override;


protected: // members

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
};


//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
