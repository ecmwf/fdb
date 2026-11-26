// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @author Simon Smart
/// @date   October 2018

#pragma once

#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "metkit/hypercube/HyperCubePayloaded.h"

#include "fdb5/api/helpers/APIIterator.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/database/Key.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

enum class ListMode {
    Full,
    Deduplicate,
    OnlyDuplicates
};

//----------------------------------------------------------------------------------------------------------------------

class ListElementDeduplicator : public metkit::hypercube::Deduplicator<ListElement> {
public:

    bool toReplace(const ListElement& existing, const ListElement& replacement) const override {
        return existing.timestamp() < replacement.timestamp();
    }
};

//----------------------------------------------------------------------------------------------------------------------

using ListAggregateIterator = APIAggregateIterator<ListElement>;

//----------------------------------------------------------------------------------------------------------------------

/// KeyStore is a specialised helper class used for deduplication of return ListElements.
///
/// Previously a std::unordered_map<Key> was used, but this significantly duplicates the components of the first/second
/// level of the key, and uses an enormous amount of memory allocated as the components of the relevant Key objects
/// (std::vector, std::pair and std::string objects). Significant computational resources was used stringising and
/// hashing these objects.
///
/// We now store a hierarchy of key components, such that the (rarely changing) first two levels of the key are only
/// stored once for each set of values. The actual values stored are the fingerprints in the format
/// <value>:<value>:... as constructed in conjunction with the schema - which are significantly smaller than the
/// Key objects, and have the advantage of being a single allocation.
///
/// The introduction of this class eliminates a class of out-of-memory kill events, and improves the performance of
/// listing the contents of the FDB

class KeyStore {

    std::unordered_map<std::string, std::unordered_map<std::string, std::unordered_set<std::string>>> fingerprints_;

public:

    bool tryInsert(const std::array<Key, 3>& keyParts);
};

//----------------------------------------------------------------------------------------------------------------------

class ListIterator : public APIIterator<ListElement> {
public:

    ListIterator(APIIterator<ListElement>&& iter, ListMode mode = ListMode::Full) :
        APIIterator<ListElement>(std::move(iter)), seenKeys_({}), mode_(mode) {}

    ListIterator(ListIterator&& iter) = default;

    ListIterator& operator=(ListIterator&& iter) = default;

    bool next(ListElement& elem);

    std::pair<size_t, eckit::Length> dumpCompact(std::ostream& out);

private:

    KeyStore seenKeys_;
    ListMode mode_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
