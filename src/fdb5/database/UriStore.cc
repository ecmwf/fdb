/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "UriStore.h"

#include <iomanip>

#include "eckit/exception/Exceptions.h"
#include "eckit/serialisation/Stream.h"

namespace fdb5 {


//----------------------------------------------------------------------------------------------------------------------


UriStore::UriStore(const eckit::PathName& directory) : next_(0), readOnly_(false), directory_(directory) {}

UriStore::~UriStore() {}


UriStore::UriStore(const eckit::PathName& directory, eckit::Stream& s) :
    next_(0), readOnly_(true), directory_(directory) {
    size_t n;

    s >> n;
    for (size_t i = 0; i < n; i++) {
        UriStore::UriID id;
        std::string p;

        s >> id;
        s >> p;
        eckit::URI uri(p);

        if ((uri.scheme() == "unix") || (uri.scheme() == "file")) {
            eckit::PathName path = uri.path();
            std::string q = path.asString();

            if (!q.empty() && q[0] != '/') {
                path = directory_ / path;
            }
            uri = eckit::URI("file", path);
        }

        paths_[id] = uri;
        ids_[uri] = id;

        next_ = std::max(id + 1, next_);
    }
}


void UriStore::encode(eckit::Stream& s) const {
    s << paths_.size();
    for (URIStore::const_iterator i = paths_.begin(); i != paths_.end(); ++i) {
        s << i->first;
        const eckit::URI& uri = i->second;
        if (uri.scheme() == "file") {
            const eckit::PathName& path = uri.path();
            s << ((path.dirName() == directory_) ? path.baseName() : path);
        }
        else {
            s << uri.asRawString();
        }
    }
}


UriStore::UriID UriStore::insert(const eckit::URI& path) {
    ASSERT(!readOnly_);


    IdStore::iterator itr = ids_.find(path);
    if (itr != ids_.end()) {
        return itr->second;
    }

    UriStore::UriID current = next_;
    next_++;
    ids_[path] = current;
    paths_[current] = path;

    return current;
}

eckit::URI UriStore::get(const UriStore::UriID id) const {
    URIStore::const_iterator itr = paths_.find(id);
    ASSERT(itr != paths_.end());
    return itr->second;
}

std::vector<eckit::URI> UriStore::paths() const {
    std::vector<eckit::URI> p;

    p.reserve(paths_.size());

    for (const auto& kv : paths_) {
        p.emplace_back(kv.second);
    }

    return p;
}

void UriStore::print(std::ostream& out) const {
    for (URIStore::const_iterator itr = paths_.begin(); itr != paths_.end(); ++itr) {
        out << itr->first << " " << itr->second << std::endl;
    }
}

void UriStore::dump(std::ostream& out, const char* indent) const {
    out << indent << "Files:" << std::endl;
    for (URIStore::const_iterator itr = paths_.begin(); itr != paths_.end(); ++itr) {
        out << indent << indent << std::setw(3) << itr->first << " => " << itr->second << std::endl;
    }
}

//----------------------------------------------------------------------------------------------------------------------


}  // namespace fdb5
