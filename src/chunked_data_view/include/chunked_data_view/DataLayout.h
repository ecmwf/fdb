/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#pragma once

#include <cstddef>

namespace chunked_data_view {

/// Describes the binary layout of a single GRIB field's value array.
/// All fields within one ViewPart are required to share the same layout.
struct DataLayout {
    size_t countValues{};    ///< Number of floating-point values in the field (e.g. grid points).
    size_t bytesPerValue{};  ///< Storage size of each value in bytes (typically 4 for float32).
};

}  // namespace chunked_data_view
