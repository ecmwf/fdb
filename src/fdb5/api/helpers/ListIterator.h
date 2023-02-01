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
#include <unordered_set>
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
    ListElement(const std::vector<Key>& keyParts, std::shared_ptr<const FieldLocation> location, time_t timestamp);
    ListElement(eckit::Stream& s);

    const std::vector<Key>& key() const { return keyParts_; }
    const FieldLocation& location() const { return *location_; }
    const time_t& timestamp() const { return timestamp_; }

    Key combinedKey() const;

    void print(std::ostream& out, bool withLocation=false, bool withLength=false) const;
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

private: // members

    std::shared_ptr<const FieldLocation> location_;
    time_t timestamp_;
};

//----------------------------------------------------------------------------------------------------------------------

using ListAggregateIterator = APIAggregateIterator<ListElement>;

using ListAsyncIterator = APIAsyncIterator<ListElement>;

//----------------------------------------------------------------------------------------------------------------------

class ListIterator : public APIIterator<ListElement> {
public:
    ListIterator(APIIterator<ListElement>&& iter, bool deduplicate=false) :
        APIIterator<ListElement>(std::move(iter)), seenKeys_({}), deduplicate_(deduplicate) {}

    ListIterator(ListIterator&& iter) :
        APIIterator<ListElement>(std::move(iter)), seenKeys_(std::move(iter.seenKeys_)), deduplicate_(iter.deduplicate_) {}

    ListIterator& operator=(ListIterator&& iter) {
        APIIterator<ListElement>::operator=(std::forward<ListIterator>(iter));
        seenKeys_ = std::move(iter.seenKeys_);
        deduplicate_ = iter.deduplicate_;
        return *this;
    }

    bool next(ListElement& elem) {
        ListElement tmp;
        while (APIIterator<ListElement>::next(tmp)) {
            if(deduplicate_) {
                Key combinedKey = tmp.combinedKey();
                if (seenKeys_.find(combinedKey) == seenKeys_.end()) {
                    seenKeys_.emplace(std::move(combinedKey));
                    std::swap(elem, tmp);
                    return true;
                }
            } else {
                std::swap(elem, tmp);
                return true;
            }
        }
        return false;
    }

private:
    std::unordered_set<Key> seenKeys_;
    bool deduplicate_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
