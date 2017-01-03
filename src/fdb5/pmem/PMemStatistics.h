/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   PMemStatistics.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_PMemStatistics_H
#define fdb5_PMemStatistics_H

#include <iosfwd>

#include "fdb5/database/DbStatistics.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class PMemStatistics : public DbStatistics {
public:

    PMemStatistics() ;

    unsigned long long poolsSize_;
    unsigned long long schemaSize_;

    size_t poolsCount_;
    size_t indexesCount_;

    PMemStatistics& operator+= (const PMemStatistics &rhs) ;

    virtual void add(const DbStatistics&);
    virtual void report(std::ostream &out, const char* indent = "") const;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
