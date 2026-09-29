/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

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
