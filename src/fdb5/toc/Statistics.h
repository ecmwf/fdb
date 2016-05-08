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

#include "eckit/io/Length.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class Statistics {
public:
    static void reportCount(std::ostream& out, const char* title, size_t value, const char* indent = "");
    static void reportBytes(std::ostream& out, const char* title, eckit::Length value, const char* indent = "");

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
