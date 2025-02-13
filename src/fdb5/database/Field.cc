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

#include "fdb5/database/FieldDetails.h"
#include "fdb5/database/FieldLocation.h"

#include <ctime>
#include <memory>
#include <ostream>
#include <utility>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Field::Field() {}

Field::Field(std::shared_ptr<const FieldLocation> location, time_t timestamp, FieldDetails details) :
    location_(std::move(location)), timestamp_(timestamp), details_(std::move(details)) {}

void Field::print(std::ostream& out) const {
    out << "Field(location=" << location_;
    if (details_) {
        out << ",details=" << details_;
    }
    out << ")";
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
