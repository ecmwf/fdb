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

::eckit::ClassSpec                    FamIndexLocation::classSpec_ = {&IndexLocation::classSpec(), "FamIndexLocation"};
::eckit::Reanimator<FamIndexLocation> FamIndexLocation::reanimator_;

//----------------------------------------------------------------------------------------------------------------------

FamIndexLocation::FamIndexLocation(const eckit::FamObjectName& object): object_ {object} { }

FamIndexLocation::FamIndexLocation(eckit::Stream& stream): object_ {stream} { }

auto FamIndexLocation::uri() const -> eckit::URI {
    return object_.uri();
}

auto FamIndexLocation::clone() const -> IndexLocation* {
    return new FamIndexLocation(object_);
}

void FamIndexLocation::encode(eckit::Stream& stream) const {
    stream << object_;
}

void FamIndexLocation::print(std::ostream& out) const {
    out << "FamIndexLocation[" << object_ << ']';
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
