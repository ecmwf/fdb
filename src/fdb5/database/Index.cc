/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/Index.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Index::Index(eckit::Stream &s, const eckit::PathName &directory, const eckit::PathName &path, off_t offset) :
    mode_(Index::READ),
    path_(path),
    offset_(offset),
    files_(directory, s),
    axes_(s),
    key_(s) {
    s >> prefix_;
    s >> type_;
}

void Index::encode(eckit::Stream &s) const {
    files_.encode(s);
    axes_.encode(s);
    s << key_;
    s << prefix_;
    s << type_;
}

Index::Index(const Key &key, const eckit::PathName &path, off_t offset, Index::Mode mode, const std::string& type ) :
    mode_(mode),
    path_(path),
    offset_(offset),
    files_(path.dirName()),
    axes_(),
    key_(key),
    prefix_(key.valuesToString()),
    type_(type) {
}

Index::~Index() {
}


void Index::put(const Key &key, const Index::Field &field) {
    axes_.insert(key);
    add(key, field);
}

const Key &Index::key() const {
    return key_;
}

const std::string &Index::type() const {
    return type_;
}
//----------------------------------------------------------------------------------------------------------------------

const eckit::PathName& Index::path() const {
    return path_;
}

off_t Index::offset() const {
    return offset_;
}

const IndexAxis &Index::axes() const {
    return axes_;
}

void Index::Field::print(std::ostream &out) const {
    out << path_ << "," << offset_ << "+" << length_ ;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
