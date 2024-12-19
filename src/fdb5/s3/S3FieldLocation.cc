/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

// #include "eckit/filesystem/URIManager.h"
#include "eckit/io/s3/S3ObjectName.h"

#include "fdb5/s3/S3FieldLocation.h"
// #include "fdb5/LibFdb5.h"

namespace fdb5 {

::eckit::ClassSpec S3FieldLocation::classSpec_ = {&FieldLocation::classSpec(), "S3FieldLocation",};
::eckit::Reanimator<S3FieldLocation> S3FieldLocation::reanimator_;

//----------------------------------------------------------------------------------------------------------------------

S3FieldLocation::S3FieldLocation(const S3FieldLocation& rhs) :
    FieldLocation(rhs.uri_, rhs.offset_, rhs.length_, rhs.remapKey_) {}

S3FieldLocation::S3FieldLocation(const eckit::URI &uri) : FieldLocation(uri) {}

/// @todo: remove remapKey from signature and always pass empty Key to FieldLocation
S3FieldLocation::S3FieldLocation(const eckit::URI &uri, eckit::Offset offset, eckit::Length length, const Key& remapKey) :
    FieldLocation(uri, offset, length, remapKey) {}

S3FieldLocation::S3FieldLocation(eckit::Stream& s) :
    FieldLocation(s) {}

std::shared_ptr<const FieldLocation> S3FieldLocation::make_shared() const {
    return std::make_shared<const S3FieldLocation>(std::move(*this));
}

eckit::DataHandle* S3FieldLocation::dataHandle() const {

    return eckit::S3ObjectName(uri_).dataHandle(offset());

}

void S3FieldLocation::print(std::ostream &out) const {
    out << "S3FieldLocation[uri=" << uri_ << "]";
}

void S3FieldLocation::visit(FieldLocationVisitor& visitor) const {
    visitor(*this);
}

static FieldLocationBuilder<S3FieldLocation> builder("s3");

} // namespace fdb5
