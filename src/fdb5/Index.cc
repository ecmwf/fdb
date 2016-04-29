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

#include "eckit/config/Resource.h"
#include "eckit/io/FileHandle.h"
#include "eckit/parser/JSON.h"
#include "eckit/parser/JSONParser.h"
#include "eckit/thread/AutoLock.h"

#include "eckit/serialisation/Stream.h"

namespace fdb5 {

//-----------------------------------------------------------------------------
//-----------------------------------------------------------------------------
Index::Index(eckit::Stream &s, const eckit::PathName &directory, const eckit::PathName &path) :
    mode_(Index::READ),
    path_(path),
    files_(directory, s),
    axes_(s),
    key_(s) {
    s >> prefix_;
}

void Index::encode(eckit::Stream& s) const
{
    files_.encode(s);
    axes_.encode(s);
    s << key_;
    s << prefix_;
}

Index::Index(const Key &key, const eckit::PathName &path, Index::Mode mode ) :
    mode_(mode),
    path_(path),
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

void Index::deleteFiles(bool doit) const
{
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
