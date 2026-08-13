/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/rados/RadosFieldLocation.h"

#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Key.h"

#include "eckit/filesystem/URIManager.h"
#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"
#include "eckit/io/rados/RadosObject.h"
#include "eckit/serialisation/Reanimator.h"
#include "eckit/serialisation/Stream.h"

#include <memory>
#include <ostream>

namespace fdb5 {

::eckit::ClassSpec RadosFieldLocation::classSpec_ = {
    &FieldLocation::classSpec(),
    "RadosFieldLocation",
};
::eckit::Reanimator<RadosFieldLocation> RadosFieldLocation::reanimator_;

//----------------------------------------------------------------------------------------------------------------------

static FieldLocationBuilder<RadosFieldLocation> builder("rados");

RadosFieldLocation::RadosFieldLocation(const RadosFieldLocation& rhs) :
    FieldLocation(rhs.uri_, rhs.offset_, rhs.length_, rhs.remapKey_) {}

RadosFieldLocation::RadosFieldLocation(const eckit::URI& uri) : FieldLocation(uri) {}

/// @todo: remove remapKey from signature and always pass empty Key to FieldLocation
RadosFieldLocation::RadosFieldLocation(const eckit::URI& uri, eckit::Offset offset, eckit::Length length,
                                       const Key& remapKey) :
    FieldLocation(uri, offset, length, remapKey) {}

RadosFieldLocation::RadosFieldLocation(eckit::Stream& s) : FieldLocation(s) {}

std::shared_ptr<const FieldLocation> RadosFieldLocation::make_shared() const {
    return std::make_shared<RadosFieldLocation>(*this);
}

eckit::DataHandle* RadosFieldLocation::dataHandle() const {

    return eckit::RadosObject(uri_).multipartRangeReadHandle(offset(), length());
}

void RadosFieldLocation::print(std::ostream& out) const {
    out << "RadosFieldLocation[uri=" << uri_ << "]";
}

void RadosFieldLocation::visit(FieldLocationVisitor& visitor) const {
    visitor(*this);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
