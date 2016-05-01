/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/Field.h"
#include "fdb5/database/FieldRef.h"
#include "fdb5/database/FileStore.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Field::Field() {

}

Field::Field(const FileStore &store, const FieldRef &ref):
    location_(store.get(ref.pathId()), ref.offset(), ref.length()) {
}

Field::Field(const eckit::PathName &path, eckit::Offset offset, eckit::Length length ):
    location_(path, offset, length) {
}

eckit::DataHandle *Field::dataHandle() const {
    return location_.dataHandle();
}

const eckit::PathName &Field::path() const {
    return location_.path();
}

const eckit::Length &Field::length() const {
    return location_.length();
}

const eckit::Offset &Field::offset() const {
    return location_.offset();
}

const FieldLocation &Field::location() const {
    return location_;
}

const FieldDetails &Field::details() const {
    return details_;
}
//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
