/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/toc/TocFieldLocation.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocFieldLocation::TocFieldLocation() {
}

TocFieldLocation::TocFieldLocation(const eckit::PathName &path, eckit::Offset offset, eckit::Length length ) :
    FieldLocation(length),
    path_(path),
    offset_(offset) {
}

TocFieldLocation::TocFieldLocation(const TocFieldLocation& rhs) :
    FieldLocation(rhs.length_),
    path_(rhs.path_),
    offset_(rhs.offset_) {
}

TocFieldLocation::TocFieldLocation(const FileStore &store, const FieldRef &ref) :
    FieldLocation(ref.length()),
    path_(store.get(ref.pathId())),
    offset_(ref.offset()) {}


eckit::SharedPtr<FieldLocation> TocFieldLocation::make_shared() const {
    return eckit::SharedPtr<FieldLocation>(new TocFieldLocation(*this));
}


eckit::DataHandle *TocFieldLocation::dataHandle() const {
    return path_.partHandle(offset_, length_);
}

eckit::PathName TocFieldLocation::url() const
{
    return path_;
}

void TocFieldLocation::print(std::ostream &out) const {
    out << "(" << path_ << "," << offset_ << "," << length_ << ")";
}

void TocFieldLocation::visit(FieldLocationVisitor& visitor) const {
    visitor(*this);
}

eckit::PathName TocFieldLocationGetter::location() const
{
    return path();
}

void TocFieldLocationGetter::operator() (const TocFieldLocation& location) {
    path_   = &location.path();
    offset_ = location.offset();
    length_ = location.length();
}


const eckit::PathName& TocFieldLocationGetter::path() const {
    return *path_;
}

eckit::Offset TocFieldLocationGetter::offset() const {
    return offset_;
}

eckit::Length TocFieldLocationGetter::length() const {
    return length_;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
