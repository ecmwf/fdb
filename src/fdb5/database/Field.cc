/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/Field.h"
#include "fdb5/database/FileStore.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Field::Field() {
}

Field::Field(const FieldLocation& location, const FieldDetails& details):
    location_(location.make_shared()),
    details_(details) {
}

void Field::print(std::ostream& out) const {
    out << "Field(location=" << location_;
    if(details_) { out << ",details=" << details_; }
    out << ")";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
