// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/daos/DaosIndexLocation.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosIndexLocation::DaosIndexLocation(const fdb5::DaosKeyValueName& name, off_t offset) : name_(name), offset_(offset) {}

void DaosIndexLocation::print(std::ostream& out) const {

    out << "(" << name_.URI().asString() << ":" << offset_ << ")";
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
