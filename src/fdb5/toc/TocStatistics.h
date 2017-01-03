/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   TocStatistics.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_TocStatistics_H
#define fdb5_TocStatistics_H

#include <iosfwd>

#include "fdb5/database/DbStatistics.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class TocStatistics : public DbStatistics {
public:

    TocStatistics();

    size_t tocRecordsCount_;

    unsigned long long tocFileSize_;
    unsigned long long schemaFileSize_;
    unsigned long long ownedFilesSize_;
    unsigned long long adoptedFilesSize_;
    unsigned long long indexFilesSize_;

    size_t ownedFilesCount_;
    size_t adoptedFilesCount_;
    size_t indexFilesCount_;

    TocStatistics& operator+= (const TocStatistics& rhs);

    virtual void add(const DbStatistics&);
    virtual void report(std::ostream &out, const char* indent = "") const;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
