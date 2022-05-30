/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/api/helpers/StatusIterator.h"

#include "eckit/filesystem/PathName.h"

#include "fdb5/database/Catalogue.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

using value_type = typename std::underlying_type<ControlIdentifier>::type;

StatusElement::StatusElement() :
    location() {}

StatusElement::StatusElement(const Catalogue& catalogue) :
    key(catalogue.key()), location(catalogue.uri()) {
        
    controlIdentifiers = ControlIdentifier::None;
    for (auto id : ControlIdentifierList) {
        if (!catalogue.enabled(id)) controlIdentifiers |= id;
    }
}

StatusElement::StatusElement(eckit::Stream &s) :
    key(s),
    location(s),
    controlIdentifiers(s) {}

void StatusElement::encode(eckit::Stream& s) const {
    s << key;
    s << location;
    s << controlIdentifiers;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

