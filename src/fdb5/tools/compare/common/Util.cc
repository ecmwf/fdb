// SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
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
