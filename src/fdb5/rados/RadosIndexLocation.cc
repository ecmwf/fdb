// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/rados/RadosIndexLocation.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

RadosIndexLocation::RadosIndexLocation(const eckit::RadosKeyValue& name, off_t offset) : name_(name), offset_(offset) {}

void RadosIndexLocation::print(std::ostream& out) const {

    out << "(" << name_.uri().asString() << ":" << offset_ << ")";
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
