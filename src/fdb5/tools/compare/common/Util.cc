/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include "Util.h"
#include <numeric>

namespace compare {

template <typename IntType, size_t NumBytes>
IntType readBigEndian(const uint8_t* p) {
    return std::accumulate(p, p + NumBytes, IntType(0), [](IntType a, uint8_t b) { return (a << 8) | b; });
}

uint32_t readU24Be(const uint8_t* p) {
    return readBigEndian<uint32_t, 3>(p);
}

uint32_t readU32Be(const uint8_t* p) {
    return readBigEndian<uint32_t, 4>(p);
}

uint64_t readU64Be(const uint8_t* p) {
    return readBigEndian<uint64_t, 8>(p);
}

}  // namespace compare
