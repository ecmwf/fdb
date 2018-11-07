/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/api/helpers/ListIterator.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

ListElement::ListElement(const std::vector<Key>& keyParts,
                               std::shared_ptr<const FieldLocation> location) :
    keyParts_(keyParts),
    location_(location) {}


void ListElement::print(std::ostream &out, bool location) const {
    for (const auto& bit : keyParts_) {
        out << bit;
    }
    if (location && location_) {
        out << " " << *location_;
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

