/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/toc/TocFieldLocation.h"
#include "TocStore.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/io/SingleGribMungePartFileHandle.h"

namespace fdb5 {

::eckit::ClassSpec TocFieldLocation::classSpec_ = {&FieldLocation::classSpec(), "TocFieldLocation",};
::eckit::Reanimator<TocFieldLocation> TocFieldLocation::reanimator_;

//----------------------------------------------------------------------------------------------------------------------

//TocFieldLocation::TocFieldLocation() {}

TocFieldLocation::TocFieldLocation(const eckit::PathName path, eckit::Offset offset, eckit::Length length ) :
        FieldLocation(eckit::URI("file", path), offset, length) {}

TocFieldLocation::TocFieldLocation(const eckit::URI &uri) :
        FieldLocation(uri) {}

TocFieldLocation::TocFieldLocation(const eckit::URI &uri, eckit::Offset offset, eckit::Length length ) :
        FieldLocation(uri, offset, length) {}

TocFieldLocation::TocFieldLocation(const TocFieldLocation& rhs) :
    FieldLocation(rhs.uri_) {}

TocFieldLocation::TocFieldLocation(const FileStore &store, const FieldRef &ref) :
    FieldLocation(store.get(ref.pathId()), ref.offset(), ref.length()) {}

TocFieldLocation::TocFieldLocation(eckit::Stream& s) :
    FieldLocation(s) {}


std::shared_ptr<FieldLocation> TocFieldLocation::make_shared() const {
    return std::make_shared<TocFieldLocation>(*this);
}

eckit::DataHandle *TocFieldLocation::dataHandle() const {
    return path().partHandle(offset(), length());
}

eckit::DataHandle *TocFieldLocation::dataHandle(const Key& remapKey) const {
    return new SingleGribMungePartFileHandle(path(), offset(), length(), remapKey);
}

void TocFieldLocation::print(std::ostream &out) const {
    out << "TocFieldLocation[uri=" << uri_ << "]";
}

void TocFieldLocation::visit(FieldLocationVisitor& visitor) const {
    visitor(*this);
}

eckit::URI TocFieldLocation::uri(const eckit::PathName &path) {
    return eckit::URI("file", path);
}

void TocFieldLocation::encode(eckit::Stream& s) const {
    LOG_DEBUG(LibFdb5::instance().debug(), LibFdb5) << "TocFieldLocation encode URI " << uri_.asRawString() << std::endl;

    FieldLocation::encode(s);
}

static FieldLocationBuilder<TocFieldLocation> builder("file");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
