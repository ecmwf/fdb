/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   DbStatistics.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_DbStatistics_H
#define fdb5_DbStatistics_H

#include <iosfwd>

#include "eckit/log/Statistics.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


class DbStatistics : public eckit::Statistics {
public:

    virtual ~DbStatistics();

    virtual void add(const DbStatistics&) = 0;

    virtual void report(std::ostream& out, const char* indent = "") const = 0;
};


//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
