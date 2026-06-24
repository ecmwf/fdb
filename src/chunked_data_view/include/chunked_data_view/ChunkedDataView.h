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
#include <vector>

namespace chunked_data_view {

/// Abstract interface for a Zarr-compatible N-dimensional chunked array backed by FDB data.
///
/// Coordinates use chunk indices: the caller identifies a chunk by its position in the
/// chunk grid (one index per dimension), and the implementation fetches and assembles the
/// corresponding field values from FDB.
///
/// The last dimension is always the implicit field-values dimension (one entry per grid
/// point in a GRIB message); it is never chunked and is always returned as a contiguous block.
class ChunkedDataView {
public:

    /// A chunk-grid index: one entry per dimension (including the implicit values dimension).
    using Index = std::vector<size_t>;

    virtual ~ChunkedDataView() = default;

    /// Fills @p data_ptr with the float values of the chunk at @p index.
    /// @p len must be at least countChunkValues().
    virtual void at(const Index& index, float* data_ptr, size_t len) = 0;

    /// Number of elements per chunk in each dimension (Zarr chunk shape).
    virtual const std::vector<size_t>& chunkShape() const = 0;

    /// Number of chunks along each dimension of the chunk grid.
    virtual const std::vector<size_t>& chunks() const = 0;

    /// Total number of elements along each dimension of the full array (Zarr array shape).
    virtual const std::vector<size_t>& shape() const = 0;

    /// Value written for array positions that are not covered by any ViewPart.
    virtual const float& fillValue() const = 0;

    /// Total number of float values stored in one chunk (product of chunkShape()).
    virtual size_t countChunkValues() const = 0;
};

}  // namespace chunked_data_view
