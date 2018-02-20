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

#ifndef fdb5_PMemDBStats_H
#define fdb5_PMemDBStats_H

#include <iosfwd>

#include "fdb5/database/DataStats.h"
#include "fdb5/database/DbStats.h"
#include "fdb5/database/IndexStats.h"
#include "fdb5/database/StatsVisitor.h"

#include "eckit/filesystem/PathName.h"

#if __cplusplus >= 201103L
#include <unordered_set>
#include <unordered_map>
#endif

namespace fdb5 {

class Index;
class Field;

namespace pmem {

class PMemDBReader;

//----------------------------------------------------------------------------------------------------------------------


class PMemDbStats : public DbStatsContent {
public:

    PMemDbStats() ;

    static DbStats make() { return DbStats(new PMemDbStats()); }

    unsigned long long dataPoolsSize_;
    unsigned long long indexPoolsSize_;
    unsigned long long schemaSize_;

    size_t dataPoolsCount_;
    size_t indexPoolsCount_;
    size_t indexesCount_;

    PMemDbStats& operator+= (const PMemDbStats &rhs) ;

    virtual void add(const DbStatsContent&);
    virtual void report(std::ostream &out, const char* indent = "") const;
};


//----------------------------------------------------------------------------------------------------------------------


class PMemIndexStats : public IndexStatsContent {
public:

    PMemIndexStats();

    size_t fieldsCount_;
    size_t duplicatesCount_;

    unsigned long long fieldsSize_;
    unsigned long long duplicatesSize_;

    PMemIndexStats& operator+= (const PMemIndexStats& rhs);

    virtual size_t fieldsCount() const { return fieldsCount_; }
    virtual size_t duplicatesCount() const { return duplicatesCount_; }

    virtual size_t fieldsSize() const { return fieldsSize_; }
    virtual size_t duplicatesSize() const { return duplicatesSize_; }

    virtual size_t addFieldsCount(size_t i) { fieldsCount_ += i; return fieldsCount_; }
    virtual size_t addDuplicatesCount(size_t i) { duplicatesCount_ += i; return duplicatesCount_; }

    virtual size_t addFieldsSize(size_t i) { fieldsSize_ += i; return fieldsSize_; }
    virtual size_t addDuplicatesSize(size_t i) { duplicatesSize_ += i; return duplicatesSize_; }

    virtual void add(const IndexStatsContent&);

    virtual void report(std::ostream &out, const char* indent) const;
};


//----------------------------------------------------------------------------------------------------------------------


class PMemDataStats : public DataStatsContent {
public:

    PMemDataStats();

    PMemDataStats& operator+= (const PMemDataStats& rhs);

    virtual void add(const DataStatsContent&);

    virtual void report(std::ostream &out, const char* indent) const;
};


//----------------------------------------------------------------------------------------------------------------------


class PMemStatsReportVisitor : public StatsReportVisitor {
public:

    PMemStatsReportVisitor(PMemDBReader& reader);
    virtual ~PMemStatsReportVisitor();

    virtual IndexStats indexStatistics() const;
    virtual DbStats    dbStatistics() const;

private: // methods



    virtual void visit(const Index& index,
                       const Field& field,
                       const std::string &indexFingerprint,
                       const std::string &fieldFingerprint);

protected: // members

// This is a significant performance optimisation. Use the std::unordered_set/map if they
// are available (i.e. if c++11 is supported). Otherwise use std::set/map. These have the
// same interface, so no code changes are required except in the class definition.
#if __cplusplus >= 201103L
    std::unordered_set<std::string> allDataPools_;
    std::unordered_set<std::string> allIndexPools_;

    std::unordered_map<std::string, size_t> indexUsage_;
    std::unordered_map<std::string, size_t> dataUsage_;

    std::unordered_set<std::string> active_;
#else
    std::set<eckit::PathName> allDataPools_;
    std::set<eckit::PathName> allIndexPools_;

    std::map<eckit::PathName, size_t> indexUsage_;
    std::map<eckit::PathName, size_t> dataUsage_;

    std::set<std::string> active_;
#endif

    std::map<Index, IndexStats> indexStats_;

    DbStats dbStats_;

    PMemDBReader& reader_;

    eckit::PathName lastDataPath_;
    eckit::PathName lastIndexPath_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5

#endif
