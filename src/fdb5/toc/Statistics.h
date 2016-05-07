/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Statistics.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_Statistics_H
#define fdb5_Statistics_H

#include <iosfwd>
#include <set>
#include <map>
#include <vector>

#include "eckit/filesystem/PathName.h"

#include "fdb5/database/Index.h"
#include "fdb5/database/Field.h"
#include "fdb5/toc/TocHandler.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class Statistics {
public:
    Statistics() ;

    size_t fields_;
    size_t duplicates_;
    // size_t dataFiles_;
    // size_t indexFiles_;

    // eckit::Length dataSize_;
    // eckit::Length indexSize_;

    // size_t tocSize_;

    eckit::Length fieldsSize_;
    eckit::Length duplicatesSize_;

    Statistics &operator+=(const Statistics &rhs) ;

    void report(std::ostream &out, const char* indent = "") const;

    friend std::ostream &operator<<(std::ostream &s, const Statistics &x) {
        x.print(s);
        return s;
    }

    void print(std::ostream &out) const;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
