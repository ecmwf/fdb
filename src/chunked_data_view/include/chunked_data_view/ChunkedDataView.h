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

class ChunkedDataView {
public:

    using Index = std::vector<size_t>;

    virtual ~ChunkedDataView() = default;
    /// Values in a field
    virtual void at(const Index& index, float* data_ptr, size_t len) = 0;
    /// Shape of a chunk
    virtual const std::vector<size_t>& chunkShape() const = 0;
    /// Number of chunks in each dimension
    virtual const std::vector<size_t>& chunks() const = 0;
    /// Shape of the dataset / number of values in each dimension
    virtual const std::vector<size_t>& shape() const = 0;


    virtual size_t countChunkValues() const = 0;
};

}  // namespace chunked_data_view
