/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @date   October 2018

#ifndef fdb5_ListIterator_H
#define fdb5_ListIterator_H

#include "fdb5/database/Key.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/api/helpers/APIIterator.h"

#include <memory>
#include <iosfwd>

namespace eckit {
    class Stream;
}

/*
 * Define a standard object which can be used to iterate the results of a
 * list() call on an arbitrary FDB object
 */

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class ListElement {
public: // methods

    ListElement() = default;
    ListElement(const std::vector<Key>& keyParts, std::shared_ptr<const FieldLocation> location);
    ListElement(eckit::Stream& s);

    Key combinedKey() const;

    void print(std::ostream& out, bool location=false) const;

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

public: // members

    std::vector<Key> keyParts_;
    std::shared_ptr<const FieldLocation> location_;
};

//----------------------------------------------------------------------------------------------------------------------

using ListIterator = APIIterator<ListElement>;

using ListAggregateIterator = APIAggregateIterator<ListElement>;

using ListAsyncIterator = APIAsyncIterator<ListElement>;

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
