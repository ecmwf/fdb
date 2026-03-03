/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include "Buffer.h"

#include <algorithm>
#include <cassert>
#include <cstddef>


namespace chunked_data_view {

void Buffer::setBits(size_t index) {
    assert(index < bitset_.size());
    bitset_[index] = true;
}
bool Buffer::filled() const {
    return std::all_of(bitset_.begin(), bitset_.end(), [](bool v) { return v; });
}
};  // namespace chunked_data_view
