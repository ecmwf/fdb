/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/toc/FieldRef.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"

#include "fdb5/fdb5_config.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/UriStore.h"
#include "fdb5/toc/TocFieldLocation.h"

#ifdef fdb5_HAVE_S3FDB
#include "fdb5/s3/S3FieldLocation.h"
#endif

namespace fdb5 {


//----------------------------------------------------------------------------------------------------------------------


FieldRefLocation::FieldRefLocation() {
}


FieldRefLocation::FieldRefLocation(UriStore &store, const Field& field) {

    const FieldLocation& loc = field.location();

    const auto* tocfloc = dynamic_cast<const TocFieldLocation*>(&loc);
#ifdef fdb5_HAVE_S3FDB
    const auto* s3floc = dynamic_cast<const S3FieldLocation*>(&loc);
    if(!tocfloc && !s3floc) {
        throw eckit::NotImplemented(
            "Field location is not of TocFieldLocation or S3FieldLocation type "
            "-- indexing other locations is not supported",
            Here());
    }
#else
    const TocFieldLocation* tocfloc = dynamic_cast<const TocFieldLocation*>(&loc);
    if(!tocfloc) {
        throw eckit::NotImplemented(
            "Field location is not of TocFieldLocation type "
            "-- indexing other locations is not supported",
            Here());
    }
#endif

    uriId_ = store.insert(loc.uri());
    length_ = loc.length();
    offset_ = loc.offset();

}

void FieldRefLocation::print(std::ostream &s) const {
    s << "FieldRefLocation(pathid=" << uriId_ << ",offset=" << offset_ << ",length=" << length_ << ")";
}

FieldRefReduced::FieldRefReduced() {

}

FieldRefReduced::FieldRefReduced(const FieldRef &other):
    location_(other.location()) {
}

void FieldRefReduced::print(std::ostream &s) const {
    s << location_;
}

FieldRef::FieldRef() {
}

FieldRef::FieldRef(UriStore &store, const Field &field):
    location_(store, field),
    details_(field.details()) {
}

FieldRef::FieldRef(const FieldRefReduced& other):
    location_(other.location()) {
}

void FieldRef::print(std::ostream &s) const {
    s << location_;
}


//----------------------------------------------------------------------------------------------------------------------


} // namespace fdb5
