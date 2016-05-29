/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   IndexStatistics.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_IndexStatistics_H
#define fdb5_IndexStatistics_H

#include <iosfwd>
#include "eckit/log/Statistics.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class IndexStatistics : public eckit::Statistics {
public:
    IndexStatistics() ;

    size_t fieldsCount_;
    size_t duplicatesCount_;
    eckit::Length fieldsSize_;
    eckit::Length duplicatesSize_;

    IndexStatistics &operator+=(const IndexStatistics &rhs) ;

    void report(std::ostream &out, const char* indent = "") const;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
