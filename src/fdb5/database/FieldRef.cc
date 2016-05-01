/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/FieldRef.h"
#include "eckit/serialisation/Stream.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/FileStore.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------
FieldRefBase::FieldRefBase() {
}

FieldRefBase::FieldRefBase(const FileStore   &store, const Field  &field):
    pathId_(store.get(field.path())),
    offset_(field.offset()),
    length_(field.length()) {

}

FieldRefBase::PathID FieldRefBase::pathId() const {
    return pathId_;
}

const eckit::Offset &FieldRefBase::offset() const {
    return offset_;
}

const eckit::Length &FieldRefBase::length() const {
    return length_;
}


//-----------------------------------------------------------------------------
FieldRefReduced::FieldRefReduced() {

}

FieldRefReduced::FieldRefReduced(const FieldRef &other):
    FieldRefBase(other) {
}

//-----------------------------------------------------------------------------
FieldRef::FieldRef() {
}


FieldRef::FieldRef(const FieldRefBase &other):
    FieldRefBase(other) {
}

FieldRef::FieldRef(const FileStore &store, const Field &field):
    FieldRefBase(store, field),
    FieldDetails(field.details()) {
}

FieldRef &FieldRef::operator=(const FieldRefBase &other) {
    FieldRefBase &self = *this;
    self = other;
    return *this;
}


} // namespace fdb5
