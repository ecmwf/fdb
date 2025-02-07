/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date Sep 2012

#ifndef fdb5_FileStore_H
#define fdb5_FileStore_H


#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"
#include "eckit/memory/NonCopyable.h"

#include <vector>

namespace eckit {
class Stream;
}

namespace fdb5 {


//----------------------------------------------------------------------------------------------------------------------


class UriStore : private eckit::NonCopyable {

public:  // types

    typedef size_t UriID;

    typedef std::map<UriStore::UriID, eckit::URI> URIStore;
    typedef std::map<eckit::URI, UriStore::UriID> IdStore;

public:  // methods

    UriStore(const eckit::PathName& directory);
    UriStore(const eckit::PathName& directory, eckit::Stream&);

    ~UriStore();

    /// Inserts the URI in the store
    /// @returns PathID associated to the provided eckit::URI
    UriStore::UriID insert(const eckit::URI& path);

    /// @returns a eckit::URI associated to the provided id
    /// @pre assumes that the eckit::filesystem/PathName.has already been inserted
    eckit::URI get(const UriStore::UriID id) const;

    /// Return all of the paths used
    std::vector<eckit::URI> paths() const;

    void dump(std::ostream& out, const char* indent) const;

    void encode(eckit::Stream& s) const;

    friend std::ostream& operator<<(std::ostream& s, const UriStore& x) {
        x.print(s);
        return s;
    }

private:  // members

    void print(std::ostream& out) const;

private:  // members

    UriStore::UriID next_;
    bool readOnly_;

    eckit::PathName directory_;
    URIStore paths_;
    IdStore ids_;
};


//----------------------------------------------------------------------------------------------------------------------


}  // namespace fdb5

#endif
