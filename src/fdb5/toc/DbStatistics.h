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

class TocHandler;

namespace pmem {
    class PMemDB;
}

//----------------------------------------------------------------------------------------------------------------------

class DbStatistics : public eckit::Statistics {
  public:
    DbStatistics() ;

    size_t tocRecordsCount_;
    unsigned long long tocFileSize_;
    unsigned long long schemaFileSize_;
    unsigned long long ownedFilesSize_;
    unsigned long long adoptedFilesSize_;
    unsigned long long indexFilesSize_;

    size_t ownedFilesCount_;
    size_t adoptedFilesCount_;
    size_t indexFilesCount_;

    DbStatistics &operator+=(const DbStatistics &rhs) ;

    void update(TocHandler&);
    void update(pmem::PMemDB&);
    void report(std::ostream &out, const char* indent = "") const;


};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
