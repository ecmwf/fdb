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

#include "fdb5/fam/FamFieldLocation.h"

#include "eckit/io/fam/FamObjectName.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/fam/FamCommon.h"

namespace fdb5 {

::eckit::ClassSpec FamFieldLocation::classSpec_ = {&FieldLocation::classSpec(), "FamFieldLocation"};

::eckit::Reanimator<FamFieldLocation> FamFieldLocation::reanimator_;

static const FieldLocationBuilder<FamFieldLocation> builder(FamCommon::typeName);

//----------------------------------------------------------------------------------------------------------------------

FamFieldLocation::FamFieldLocation(const eckit::URI& uri) : FieldLocation(uri) {}

FamFieldLocation::FamFieldLocation(const eckit::URI& uri, const eckit::Offset& offset, const eckit::Length& length,
                                   const Key& remapKey) :
    FieldLocation(uri, offset, length, remapKey) {}

FamFieldLocation::FamFieldLocation(const FamFieldLocation& rhs) :
    FieldLocation(rhs.uri_, rhs.offset_, rhs.length_, rhs.remapKey_) {}

FamFieldLocation::FamFieldLocation(eckit::Stream& stream) : FieldLocation(stream) {}

std::shared_ptr<const FieldLocation> FamFieldLocation::make_shared() const {
    return std::make_shared<FamFieldLocation>(*this);
}

eckit::DataHandle* FamFieldLocation::dataHandle() const {
    return eckit::FamObjectName(uri_).dataHandle(offset(), length());
}

void FamFieldLocation::print(std::ostream& out) const {
    out << "FamFieldLocation[uri=" << uri_ << ", offset=" << offset() << ", length=" << length()
        << ", remapKey=" << remapKey_ << ']';
}

void FamFieldLocation::visit(FieldLocationVisitor& visitor) const {
    visitor(*this);
}

void FamFieldLocation::encode(eckit::Stream& s) const {
    LOG_DEBUG_LIB(LibFdb5) << "FamFieldLocation encode URI " << uri_.asRawString() << '\n';
    FieldLocation::encode(s);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
