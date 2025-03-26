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

#include <unordered_set>
#include <utility>

#include "metkit/hypercube/HyperCubePayloaded.h"

#include "fdb5/api/helpers/APIIterator.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/database/Key.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class ListElementDeduplicator : public metkit::hypercube::Deduplicator<ListElement> {
public:

    bool toReplace(const ListElement& existing, const ListElement& replacement) const override {
        return existing.timestamp() < replacement.timestamp();
    }
};

//----------------------------------------------------------------------------------------------------------------------

using ListAggregateIterator = APIAggregateIterator<ListElement>;

using ListAsyncIterator = APIAsyncIterator<ListElement>;

//----------------------------------------------------------------------------------------------------------------------

class ListIterator : public APIIterator<ListElement> {
public:

    ListIterator(APIIterator<ListElement>&& iter, bool deduplicate = false) :
        APIIterator<ListElement>(std::move(iter)), seenKeys_({}), deduplicate_(deduplicate) {}

    ListIterator(ListIterator&& iter) = default;

    ListIterator& operator=(ListIterator&& iter) = default;

    bool next(ListElement& elem);

    std::pair<size_t, eckit::Length> dumpCompact(std::ostream& out);

private:

    std::unordered_set<Key> seenKeys_;
    bool deduplicate_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif
