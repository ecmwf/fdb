/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/FileStore.h"
#include "eckit/serialisation/Stream.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

void FileStore::FieldRef::load(std::istream &s) {
    long long offset;
    long long length;
    s >> pathId_ >> offset >> length;
    offset_  = offset;
    length_  = length;
}

void FileStore::FieldRef::dump(std::ostream &s) const {
    s << pathId_ << " " << offset_ << " " << length_;
}

//----------------------------------------------------------------------------------------------------------------------

FileStore::FileStore(const eckit::PathName &tocDir) :
    next_(0),
    readOnly_(false),
    tocDir_(tocDir) {
}

FileStore::~FileStore() {
}


FileStore::FileStore(const eckit::PathName &tocDir, eckit::Stream &s):
    next_(0),
    readOnly_(true),
    tocDir_(tocDir) {
    size_t n;

    s >> n;
    for (size_t i = 0; i < n ; i++) {
        FileStore::PathID id;
        std::string p;

        s >> id;
        s >> p;

        eckit::PathName path(p);

        if (!p.empty() && p[0] != '/') {
            path = tocDir_ / path;
        }

        paths_[id] = path;
        ids_[path] = id;
        next_ = std::max(id + 1, next_);
    }

}


void FileStore::encode(eckit::Stream &s) const {
    s << paths_.size();
    for ( PathStore::const_iterator i = paths_.begin(); i != paths_.end(); ++i ) {
        s << i->first;
        const eckit::PathName &path = i->second;
        s << ( (path.dirName() == tocDir_) ?  path.baseName() : path );
    }
}


FileStore::PathID FileStore::insert( const eckit::PathName &path ) {
    ASSERT(!readOnly_);

    IdStore::iterator itr = ids_.find(path);
    if ( itr != ids_.end() )
        return itr->second;

    FileStore::PathID current = next_;
    next_++;
    ids_[path] = current;
    paths_[current] = path;

    return current;
}

FileStore::PathID FileStore::get( const eckit::PathName &path ) const {
    IdStore::const_iterator itr = ids_.find(path);
    ASSERT( itr != ids_.end() );
    return itr->second;
}

eckit::PathName FileStore::get(const FileStore::PathID id) const {
    PathStore::const_iterator itr = paths_.find(id);
    ASSERT( itr != paths_.end() );
    return itr->second;
}

bool FileStore::exists(const PathID id ) const {
    PathStore::const_iterator itr = paths_.find(id);
    return ( itr != paths_.end() );
}

bool FileStore::exists(const eckit::PathName &path) const {
    IdStore::const_iterator itr = ids_.find(path);
    return ( itr != ids_.end() );
}

void FileStore::print( std::ostream &out ) const {
    for ( PathStore::const_iterator itr = paths_.begin(); itr != paths_.end(); ++itr ) {
        out << itr->first << " " << itr->second << std::endl;
    }
}

//-----------------------------------------------------------------------------

} // namespace fdb5
