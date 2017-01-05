/*
 * (C) Copyright 1996-2016 ECMWF.
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

#include "fdb5/database/DbStats.h"
#include "fdb5/database/IndexStats.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


class PMemDbStats : public DbStatsContent {
public:

    PMemDbStats() ;

    unsigned long long poolsSize_;
    unsigned long long schemaSize_;

    size_t poolsCount_;
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

    virtual void add(const IndexStatsContent&);

    virtual void report(std::ostream &out, const char* indent) const;
};


//----------------------------------------------------------------------------------------------------------------------


class PMemDataStats : public IndexStatsContent {
public:

    PMemDataStats();

    PMemDataStats& operator+= (const PMemDataStats& rhs);

    virtual void add(const IndexStatsContent&);

    virtual void report(std::ostream &out, const char* indent) const;
};


//----------------------------------------------------------------------------------------------------------------------


} // namespace fdb5

#endif
