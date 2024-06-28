/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/rados/RadosIndexLocation.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

// #if defined(fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_WRITE) || defined(fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_FLUSH)
// RadosIndexLocation::RadosIndexLocation(const eckit::RadosPersistentKeyValue& name, off_t offset) : name_(name), offset_(offset) {}
// #else
RadosIndexLocation::RadosIndexLocation(const eckit::RadosKeyValue& name, off_t offset) : name_(name), offset_(offset) {}
// #endif

void RadosIndexLocation::print(std::ostream &out) const {

    out << "(" << name_.uri().asString() << ":" << offset_ << ")";

}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
