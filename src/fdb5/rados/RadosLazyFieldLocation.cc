/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/serialisation/MemoryStream.h"

#include "fdb5/rados/RadosLazyFieldLocation.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

RadosLazyFieldLocation::RadosLazyFieldLocation(const fdb5::RadosLazyFieldLocation& rhs) :
    FieldLocation(), index_(rhs.index_), key_(rhs.key_) {}

RadosLazyFieldLocation::RadosLazyFieldLocation(const eckit::RadosKeyValue& index, const std::string& key) :
    FieldLocation(), index_(index), key_(key) {}

std::shared_ptr<FieldLocation> RadosLazyFieldLocation::make_shared() const {
    return std::make_shared<RadosLazyFieldLocation>(std::move(*this));
}

eckit::DataHandle* RadosLazyFieldLocation::dataHandle() const {

    return realise()->dataHandle();
    
}

void RadosLazyFieldLocation::print(std::ostream &out) const {
    out << *realise();
}

void RadosLazyFieldLocation::visit(FieldLocationVisitor& visitor) const {
    realise()->visit(visitor);
}

std::shared_ptr<FieldLocation> RadosLazyFieldLocation::stableLocation() const {
    return realise()->make_shared();
}

std::unique_ptr<fdb5::FieldLocation>& RadosLazyFieldLocation::realise() const {

    if (fl_) return fl_;

    /// @note: performed RPCs:
    /// - index kv get (daos_kv_get)
    std::vector<char> data;
    eckit::MemoryStream ms = index_.getMemoryStream(data, key_, "index kv");

    /// @note: timestamp read for informational purpoes. See note in DaosIndex::add.
    time_t ts;
    ms >> ts;

    fl_.reset(eckit::Reanimator<fdb5::FieldLocation>::reanimate(ms));

    return fl_;

}

} // namespace fdb5
