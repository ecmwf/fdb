// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/*
 * This software was developed as part of the Horizon Europe programme funded project OpenCUBE
 * (Grant agreement: 101092984) horizon-opencube.eu
 */

#include "fdb5/fam/FamFieldLocation.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/FieldLocation.h"

#include "eckit/io/fam/FamTypes.h"

namespace fdb5 {

::eckit::ClassSpec FamFieldLocation::classSpec_ = {&FieldLocation::classSpec(), "FamFieldLocation"};

::eckit::Reanimator<FamFieldLocation> FamFieldLocation::reanimator_;

static const FieldLocationBuilder<FamFieldLocation> builder(eckit::fam::scheme);

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
    return uri_.newReadHandle({offset()}, {length()});
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
