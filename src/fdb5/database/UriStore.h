// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date Sep 2012

#pragma once


#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"

#include <vector>

namespace eckit {
class Stream;
}

namespace fdb5 {


//----------------------------------------------------------------------------------------------------------------------


class UriStore {

public:  // types

    typedef size_t UriID;

    typedef std::map<UriStore::UriID, eckit::URI> URIStore;
    typedef std::map<eckit::URI, UriStore::UriID> IdStore;

public:  // methods

    UriStore(const eckit::PathName& directory);
    UriStore(const eckit::PathName& directory, eckit::Stream&);

    UriStore(const UriStore&) = delete;
    UriStore& operator=(const UriStore&) = delete;
    UriStore(UriStore&&) = delete;
    UriStore& operator=(UriStore&&) = delete;

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
