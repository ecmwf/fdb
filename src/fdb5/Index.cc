/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/Index.h"


namespace fdb5 {

//-----------------------------------------------------------------------------
//-----------------------------------------------------------------------------
Index::Index(eckit::Stream &s, const eckit::PathName &directory, const eckit::PathName &path, off_t offset) :
    mode_(Index::READ),
    path_(path),
    offset_(offset),
    files_(directory, s),
    axes_(s),
    key_(s) {
    s >> prefix_;
}

void Index::encode(eckit::Stream &s) const {
    files_.encode(s);
    axes_.encode(s);
    s << key_;
    s << prefix_;
}

Index::Index(const Key &key, const eckit::PathName &path, off_t offset, Index::Mode mode ) :
    mode_(mode),
    path_(path),
    offset_(offset),
    files_(path.dirName()),
    axes_(),
    key_(key),
    prefix_(key.valuesToString()) {
}

Index::~Index() {
    flush();
}

void Index::flush() {
}

void Index::put(const Key &key, const Index::Field &field) {
    axes_.insert(key);
    put_(key, field);
}

//-----------------------------------------------------------------------------

void Index::Field::load(std::istream &s) {
    std::string spath;
    long long offset;
    long long length;
    s >> spath >> offset >> length;
    path_    = spath;
    offset_  = offset;
    length_  = length;
}

void Index::Field::dump(std::ostream &s) const {
    s << path_ << " " << offset_ << " " << length_;
}

//-----------------------------------------------------------------------------

const Key &Index::key() const {
    return key_;
}

//-----------------------------------------------------------------------------



} // namespace fdb5
