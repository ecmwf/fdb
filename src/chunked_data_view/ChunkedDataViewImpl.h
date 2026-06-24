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
#include "chunked_data_view/Extractor.h"
#include "chunked_data_view/ViewPart.h"

#include <cstddef>
#include <vector>

namespace chunked_data_view {

/// Concrete implementation of ChunkedDataView backed by one or more ViewParts.
///
/// At construction the combined shape, chunk shape, and chunk-grid dimensions are derived
/// from the supplied parts. at() then intersects the requested chunk with each part's
/// bounding box and delegates retrieval to the corresponding Extractor.
class ChunkedDataViewImpl : public ChunkedDataView {
public:

    ChunkedDataViewImpl(std::vector<std::pair<ViewPart, std::shared_ptr<Extractor>>>& partialViews, float fillValue,
                        size_t extensionAxisIndex);

    /// Fills @p ptr with the float values of the chunk at @p chunkIndex.
    /// Each part that overlaps the chunk contributes its fields; positions not covered by
    /// any part are left at fillValue_.
    void at(const std::vector<size_t>& chunkIndex, float* ptr, size_t len) override;

    const std::vector<size_t>& chunkShape() const override { return chunkShape_; }
    const std::vector<size_t>& chunks() const override { return chunks_; }
    const std::vector<size_t>& shape() const override { return chunkedDataViewShape_; }
    const float& fillValue() const override { return fillValue_; }

    /// Product of all entries in chunkShape_ (field slots × values per field).
    size_t countChunkValues() const override {
        size_t result = 1;
        for (auto i : chunkShape_) {
            result *= i;
        }
        return result;
    }

    /// Number of GRIB messages expected in one chunk (product of all chunkShape_ dimensions
    /// except the implicit last field-values dimension).
    size_t countFields() const {
        size_t result = 1;
        for (size_t i = 0; i < chunkShape_.size() - 1; ++i) {
            result *= chunkShape_[i];
        }
        return result;
    }


private:  // members

    std::vector<size_t> chunkShape_{};
    std::vector<size_t> chunkedDataViewShape_{};
    std::vector<size_t> chunks_{};
    std::vector<std::pair<ViewPart, std::shared_ptr<Extractor>>> parts_{};
    size_t extensionAxisIndex_{};
    float fillValue_;

private:  // methods

    /// Computes chunkShape_ from the parts, summing extensible-axis extents across all parts.
    std::vector<size_t> chunkShape(const std::vector<std::pair<ViewPart, std::shared_ptr<Extractor>>>& parts);
};

}  // namespace chunked_data_view
