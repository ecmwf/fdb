// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/*
 * This software was developed as part of the Horizon Europe programme funded project OpenCUBE
 * (Grant agreement: 101092984) horizon-opencube.eu
 */

#include "fdb5/fam/FamIndexLocation.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

FamIndexLocation::FamIndexLocation(eckit::URI uri) : uri_(std::move(uri)) {}

void FamIndexLocation::print(std::ostream& out) const {
    out << "FamIndexLocation[uri=" << uri_ << "]";
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
