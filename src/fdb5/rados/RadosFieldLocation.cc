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
#include "fdb5/LibFdb5.h"
#include "fdb5/io/SingleGribMungePartFileHandle.h"

namespace fdb5 {

::eckit::ClassSpec RadosFieldLocation::classSpec_ = {&FieldLocation::classSpec(), "RadosFieldLocation",};
::eckit::Reanimator<RadosFieldLocation> RadosFieldLocation::reanimator_;

//----------------------------------------------------------------------------------------------------------------------

//RadosFieldLocation::RadosFieldLocation() {}

RadosFieldLocation::RadosFieldLocation(const eckit::PathName path, eckit::Offset offset, eckit::Length length ) :
        FieldLocation(eckit::URI("file", path), offset, length) {}

RadosFieldLocation::RadosFieldLocation(const eckit::URI &uri) :
        FieldLocation(uri) {}

RadosFieldLocation::RadosFieldLocation(const eckit::URI &uri, eckit::Offset offset, eckit::Length length ) :
        FieldLocation(uri, offset, length) {}

RadosFieldLocation::RadosFieldLocation(const RadosFieldLocation& rhs) :
    FieldLocation(rhs.uri_) {}

RadosFieldLocation::RadosFieldLocation(const FileStore &store, const FieldRef &ref) :
    FieldLocation(eckit::URI("rados", store.get(ref.pathId())), ref.offset(), ref.length()) {}

RadosFieldLocation::RadosFieldLocation(eckit::Stream& s) :
    FieldLocation(s) {}


std::shared_ptr<FieldLocation> RadosFieldLocation::make_shared() const {
    return std::make_shared<RadosFieldLocation>(*this);
}

eckit::DataHandle *RadosFieldLocation::dataHandle() const {
    return path().partHandle(offset(), length());
}

eckit::DataHandle *RadosFieldLocation::dataHandle(const Key& remapKey) const {
    return new SingleGribMungePartFileHandle(path(), offset(), length(), remapKey);
}

void RadosFieldLocation::print(std::ostream &out) const {
    out << "RadosFieldLocation[uri=" << uri_ << "]";
}

void RadosFieldLocation::visit(FieldLocationVisitor& visitor) const {
    visitor(*this);
}

eckit::URI RadosFieldLocation::uri(const eckit::PathName &path) {
    return eckit::URI("rados", path);
}

static FieldLocationBuilder<RadosFieldLocation> builder("rados");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
