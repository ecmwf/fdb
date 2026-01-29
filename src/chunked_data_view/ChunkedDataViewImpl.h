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

#include "chunked_data_view/ChunkedDataView.h"
#include "chunked_data_view/NDCoord.h"
#include "chunked_data_view/ViewPart.h"

#include <cstddef>
#include <vector>

namespace chunked_data_view {

class ChunkedDataViewImpl : public ChunkedDataView {
public:

    ChunkedDataViewImpl(std::vector<ViewPart> partialViews, size_t extensionAxisIndex);

    /// @param index n-dim chunk index
    void at(const ChunkIndex& index, float* ptr, size_t len) override;
    const Shape& chunkShape() const override { return chunkShape_; }
    const Shape& chunks() const override { return chunks_; }
    const Shape& shape() const override { return shape_; }

    /**
     * @brief Returns the number of entries in a chunk including the implicit field entries
     *
     * @return number of entries
     */
    size_t countChunkValues() const override {
        size_t result = 1;
        for (auto i : chunkShape_) {
            result *= i;
        }
        return result;
    }


    size_t countFields() const {
        size_t result = 1;
        for (size_t i = 0; i < chunkShape_.ndim() - 1; ++i) {
            result *= chunkShape_[i];
        }
        return result;
    }

private:

    Shape chunkShape_{};
    Shape shape_{};
    Shape chunks_{};
    std::vector<ViewPart> parts_{};
    size_t extensionAxisIndex_{};
};

}  // namespace chunked_data_view
