/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/toc/Statistics.h"

#include "eckit/log/Bytes.h"
#include "eckit/log/Plural.h"

#include "fdb5/toc/TocIndex.h"
#include "fdb5/toc/TocHandler.h"

namespace fdb5 {

const size_t WIDTH = 30;

//----------------------------------------------------------------------------------------------------------------------
void Statistics::reportCount(std::ostream& out, const char* title, size_t value, const char* indent) {
    out << indent << title << std::setw(WIDTH - strlen(title)) << " : "  << eckit::BigNum(value) << std::endl;

}

void Statistics::reportBytes(std::ostream& out, const char* title, eckit::Length value, const char* indent) {
    out << indent << title << std::setw(WIDTH - strlen(title)) << " : "  << eckit::BigNum(value) << " (" << eckit::Bytes(value) << ")" << std::endl;

}



//----------------------------------------------------------------------------------------------------------------------

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
