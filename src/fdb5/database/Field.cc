// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/database/Field.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Field::Field() {}

Field::Field(std::shared_ptr<const FieldLocation> location, time_t timestamp, const FieldDetails& details) :
    location_(std::move(location)), timestamp_(timestamp), details_(details) {}

Field::Field(const FieldLocation&& location, time_t timestamp, const FieldDetails& details) :
    location_(location.make_shared()), timestamp_(timestamp), details_(details) {}

void Field::print(std::ostream& out) const {
    out << "Field(location=" << location_;
    if (details_) {
        out << ",details=" << details_;
    }
    out << ")";
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
