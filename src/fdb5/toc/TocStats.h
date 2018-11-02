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
#include "fdb5/database/StatsVisitor.h"
#include "fdb5/database/Index.h"

#if __cplusplus >= 201103L
#include <unordered_set>
#include <unordered_map>
#endif

namespace fdb5 {

class TocDBReader;

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


class TocStatsReportVisitor : public StatsReportVisitor {
public:

    TocStatsReportVisitor(TocDBReader& reader);
    virtual ~TocStatsReportVisitor();

    virtual IndexStats indexStatistics() const;
    virtual DbStats    dbStatistics() const;

private: // methods

    virtual void visitDatum(const Field& field, const std::string& keyFingerprint);
    virtual void visitDatum(const Field& field, const Key& key) { NOTIMP; }

protected: // members

    eckit::PathName directory_;

// This is a significant performance optimisation. Use the std::unordered_set/map if they
// are available (i.e. if c++11 is supported). Otherwise use std::set/map. These have the
// same interface, so no code changes are required except in the class definition.
#if __cplusplus >= 201103L
    std::unordered_set<std::string> allDataFiles_;
    std::unordered_set<std::string> allIndexFiles_;

    std::unordered_map<std::string, size_t> indexUsage_;
    std::unordered_map<std::string, size_t> dataUsage_;

    std::unordered_set<std::string> active_;
#else
    std::set<eckit::PathName> allDataFiles_;
    std::set<eckit::PathName> allIndexFiles_;

    std::map<eckit::PathName, size_t> indexUsage_;
    std::map<eckit::PathName, size_t> dataUsage_;

    std::set<std::string> active_;
#endif

    std::map<Index, IndexStats> indexStats_;

    DbStats dbStats_;

    TocDBReader& reader_;

    eckit::PathName lastDataPath_;
    eckit::PathName lastIndexPath_;
};


//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
