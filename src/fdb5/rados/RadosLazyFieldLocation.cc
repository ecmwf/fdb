// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/rados/RadosLazyFieldLocation.h"

#include "fdb5/database/FieldLocation.h"
#include "fdb5/rados/RadosIndex.h"

#include "eckit/io/rados/RadosKeyValue.h"

#include <memory>
#include <ostream>
#include <string>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

RadosLazyFieldLocation::RadosLazyFieldLocation(const fdb5::RadosLazyFieldLocation& rhs) :
    index_(rhs.index_), key_(rhs.key_) {}

RadosLazyFieldLocation::RadosLazyFieldLocation(const eckit::RadosKeyValue& index, const std::string& key) :
    index_(index), key_(key) {}

std::shared_ptr<const FieldLocation> RadosLazyFieldLocation::make_shared() const {
    return std::make_shared<RadosLazyFieldLocation>(*this);
}

eckit::DataHandle* RadosLazyFieldLocation::dataHandle() const {

    return realise().dataHandle();
}

void RadosLazyFieldLocation::print(std::ostream& out) const {
    out << realise();
}

void RadosLazyFieldLocation::visit(FieldLocationVisitor& visitor) const {
    realise().visit(visitor);
}

std::shared_ptr<const FieldLocation> RadosLazyFieldLocation::stableLocation() const {
    realise();
    return fl_;
}

const FieldLocation& RadosLazyFieldLocation::realise() const {

    if (!fl_) {
        fl_ = RadosIndex::readEntry(index_, key_).location;
    }

    return *fl_;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
