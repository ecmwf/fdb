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

StatusElement::StatusElement() :
    location() {}

StatusElement::StatusElement(const Catalogue& catalogue) :
    key(catalogue.key()),
    location(catalogue.uri()),
    retrieveLocked(catalogue.retrieveLocked()),
    archiveLocked(catalogue.archiveLocked()),
    listLocked(catalogue.listLocked()),
    wipeLocked(catalogue.wipeLocked()) {}

StatusElement::StatusElement(eckit::Stream &s) :
    key(s),
    location(s) {
    s >> retrieveLocked;
    s >> archiveLocked;
    s >> listLocked;
    s >> wipeLocked;
}

void StatusElement::encode(eckit::Stream& s) const {
    s << key;
    s << location;
    s << retrieveLocked;
    s << archiveLocked;
    s << listLocked;
    s << wipeLocked;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

