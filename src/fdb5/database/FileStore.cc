/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "FileStore.h"

#include <iomanip>

#include "eckit/exception/Exceptions.h"
#include "eckit/serialisation/Stream.h"

namespace fdb5 {


//----------------------------------------------------------------------------------------------------------------------


FileStore::FileStore(const eckit::URI &baseUri) :
    next_(0),
    readOnly_(false),
    uri_(baseUri) {
}

FileStore::~FileStore() {
}

FileStore::FileStore(const eckit::URI &baseUri, eckit::Stream &s):
    next_(0),
    readOnly_(true),
    uri_(baseUri) {

    size_t n;
    eckit::PathName directory = baseUri.path().dirName();

    s >> n;

    for (size_t i = 0; i < n ; i++) {
        FileStore::PathID id;
        std::string p;

        s >> id;
        s >> p;

        eckit::URI uri(p);

        if (uri.scheme() == "unix") { // backward compatibility - handling absolute and/or relative paths
            eckit::PathName path(p);
            if (!p.empty() && p[0] != '/') {
                path = directory / path;
            } else {
                path = directory;
            }
            eckit::URI uriNewPath("file", path);

            uris_[id] = uriNewPath;
            ids_[uriNewPath] = id;
        } else {
            uris_[id] = uri;
            ids_[uri] = id;
        }

        next_ = std::max(id + 1, next_);
    }
}


void FileStore::encode(eckit::Stream &s) const {
    s << uris_.size();
    for ( UriStore::const_iterator i = uris_.begin(); i != uris_.end(); ++i ) {
        s << i->first;
        const eckit::URI &uri = i->second;

        s << uri.asRawString();
//        s << ( (uri == uri_) ?  uri.asRawString() : path );
    }
}

FileStore::PathID FileStore::insert( const eckit::URI &uri ) {
    ASSERT(!readOnly_);

    IdStore::iterator itr = ids_.find(uri);
    if ( itr != ids_.end() )
        return itr->second;

    FileStore::PathID current = next_;
    next_++;
    ids_[uri] = current;
    uris_[current] = uri;

    return current;
}

eckit::URI FileStore::get(const FileStore::PathID id) const {
    UriStore::const_iterator itr = uris_.find(id);
    ASSERT( itr != uris_.end() );
    return itr->second;
}

std::vector<eckit::URI> FileStore::uris() const {
    std::vector<eckit::URI> p;

    p.reserve(uris_.size());

    for (const auto& kv : uris_) {
        p.emplace_back(kv.second);
    }

    return p;
}

void FileStore::print( std::ostream &out ) const {
    for ( UriStore::const_iterator itr = uris_.begin(); itr != uris_.end(); ++itr ) {
        out << itr->first << " " << itr->second << std::endl;
    }
}

void FileStore::dump(std::ostream &out, const char* indent) const {
    out << indent << "Files:" << std::endl;
    for ( UriStore::const_iterator itr = uris_.begin(); itr != uris_.end(); ++itr ) {
        out << indent << indent << std::setw(3) << itr->first << " => " << itr->second << std::endl;
    }
}

//----------------------------------------------------------------------------------------------------------------------


} // namespace fdb5
