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

#include "fdb5/toc/Statistics.h"

namespace fdb5 {

class TocHandler;

//----------------------------------------------------------------------------------------------------------------------

class DbStatistics : public Statistics {
public:
    DbStatistics() ;

    size_t tocRecords_;
    eckit::Length tocSize_;
    eckit::Length schemaSize_;
    eckit::Length totalDataFiles_;
    eckit::Length totalAdoptedFiles_;
    eckit::Length totalIndexFiles_;

    DbStatistics &operator+=(const DbStatistics &rhs) ;

    void update(TocHandler&);
    void report(std::ostream &out, const char* indent = "") const;


};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
