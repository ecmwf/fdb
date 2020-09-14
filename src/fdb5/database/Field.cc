/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

// #include <iomanip>

#include "fdb5/database/Field.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Field::Field() {
}

Field::Field(const FieldLocation& location, timestamp_t timestamp, const FieldDetails& details):
    location_(location.make_shared()),
    timestamp_(timestamp),
    details_(details) {
}

void Field::print(std::ostream& out) const {
    out << "Field(location=" << location_;
    if(details_) { out << ",details=" << details_; }
    out << ")";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
