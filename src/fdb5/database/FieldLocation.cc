/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/FieldLocation.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------
FieldLocation::FieldLocation() {
}

FieldLocation::FieldLocation(const eckit::PathName &path, eckit::Offset offset, eckit::Length length ) :
    path_(path),
    offset_(offset),
    length_(length) {

}


eckit::DataHandle *FieldLocation::dataHandle() const {
    return path_.partHandle(offset_, length_);
}

void FieldLocation::print(std::ostream &out) const {
    out << "(" << path_ << "," << offset_ << "," << length_ << ")";
}

const eckit::PathName &FieldLocation::path() const {
    return path_;
}

const eckit::Offset &FieldLocation::offset() const {
    return offset_;
}

const eckit::Length &FieldLocation::length() const {
    return length_;
}
//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
