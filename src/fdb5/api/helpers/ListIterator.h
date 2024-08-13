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

#include "fdb5/database/Key.h"
#include "fdb5/api/helpers/APIIterator.h"
#include "fdb5/api/helpers/ListElement.h"

namespace fdb5 {

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
        seenKeys_ = std::move(iter.seenKeys_);
        deduplicate_ = iter.deduplicate_;
        APIIterator<ListElement>::operator=(std::move(iter));
        return *this;
    }

    bool next(ListElement& elem) {
        ListElement tmp;
        while (APIIterator<ListElement>::next(tmp)) {
            if(deduplicate_) {
                if (const auto& [iter, success] = seenKeys_.insert(tmp.keys().combine()); !success) { continue; }
            }
            std::swap(elem, tmp);
            return true;
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
