// SPDX-FileCopyrightText: 2025 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstddef>

namespace chunked_data_view {

/// Describes the binary layout of a single GRIB field's value array.
/// All fields within one ViewPart are required to share the same layout.
struct DataLayout {
    size_t countValues{};       ///< Number of floating-point values in the field (e.g. grid points).
    size_t bytesPerValue{};     ///< Storage size of each value in bytes (typically 4 for float32).
    size_t countChunkValues{};  ///< Number of floating-point values in the chunk (e.g. grid points).
};

}  // namespace chunked_data_view
