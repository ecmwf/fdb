/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @author Simon Smart
/// @date   October 2018

#ifndef fdb5_ListIterator_H
#define fdb5_ListIterator_H

#include <vector>
#include <memory>
#include <iosfwd>
#include <chrono>

#include "fdb5/database/Key.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/api/helpers/APIIterator.h"

namespace eckit {
    class Stream;
    class JSON;
}

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// Define a standard object which can be used to iterate the results of a
/// list() call on an arbitrary FDB object

class ListElement {
public: // methods

    ListElement() = default;
    ListElement(const std::vector<Key>& keyParts, std::shared_ptr<const FieldLocation> location, std::chrono::system_clock::time_point timestamp);
    ListElement(eckit::Stream& s);

    Key combinedKey() const;
    std::shared_ptr<const FieldLocation> location() { return location_; }
    std::chrono::system_clock::time_point timestamp() { return timestamp_; }

    void print(std::ostream& out, bool location=false) const;

    void json(eckit::JSON& json) const;

private: // methods

    void encode(eckit::Stream& s) const;

    friend std::ostream& operator<<(std::ostream& os, const ListElement& e) {
        e.print(os);
        return os;
    }

    friend eckit::Stream& operator<<(eckit::Stream& s, const ListElement& r) {
        r.encode(s);
        return s;
    }

    friend eckit::JSON& operator<<(eckit::JSON& j, const ListElement& e) {
        e.json(j);
        return j;
    }

public: // members

    std::vector<Key> keyParts_;
    std::shared_ptr<const FieldLocation> location_;
    std::chrono::system_clock::time_point timestamp_;
};

//----------------------------------------------------------------------------------------------------------------------

using ListIterator = APIIterator<ListElement>;

using ListAggregateIterator = APIAggregateIterator<ListElement>;

using ListAsyncIterator = APIAsyncIterator<ListElement>;

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
