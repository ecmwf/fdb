/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/toc/TocFieldLocation.h"
#include "fdb5/io/SingleGribMungePartFileHandle.h"
#include "fdb5/LibFdb5.h"

namespace fdb5 {

::eckit::ClassSpec TocFieldLocation::classSpec_ = {&FieldLocation::classSpec(), "TocFieldLocation",};
::eckit::Reanimator<TocFieldLocation> TocFieldLocation::reanimator_;

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

TocFieldLocation::TocFieldLocation(eckit::Stream& s) :
    FieldLocation(s) {
    s >> path_;
    s >> offset_;
}


std::shared_ptr<FieldLocation> TocFieldLocation::make_shared() const {
    return std::make_shared<TocFieldLocation>(*this);
}


eckit::DataHandle *TocFieldLocation::dataHandle(const Key& remapKey) const {
    eckit::Log::debug<LibFdb5>() << "DH: " << path_ << " " << offset_ << " " << length_ << " " << remapKey;
    if (remapKey.empty()) {
        return path_.partHandle(offset_, length_);
    } else {
        return new SingleGribMungePartFileHandle(path_, offset_, length_, remapKey);
    }
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

void TocFieldLocation::encode(eckit::Stream& s) const {
    FieldLocation::encode(s);
    s << path_;
    s << offset_;
}

void TocFieldLocation::dump(std::ostream& out) const
{
    out << "  path: " << path_ << std::endl;
    out << "  offset: " << offset_ << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
