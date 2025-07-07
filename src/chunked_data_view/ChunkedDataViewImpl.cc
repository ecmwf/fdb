/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include "ChunkedDataViewImpl.h"

namespace chunked_data_view {

ChunkedDataViewImpl::ChunkedDataViewImpl(std::vector<ViewPart> parts, size_t extensionAxisIndex) :
    parts_(std::move(parts)), extensionAxisIndex_(extensionAxisIndex) {
    shape_ = parts_[0].shape();
    for (const auto& part : parts_) {
        for (size_t idx = 0; idx < shape_.size(); ++idx) {
            if (idx == extensionAxisIndex_) {
                shape_[idx] += part.shape()[idx];
            }
            else if (shape_[idx] != part.shape()[idx]) {
                throw std::runtime_error("ChunkedDataViewImpl: Axis size mismatch");
            }
        }
    }
    shape_[extensionAxisIndex_] -= parts_[0].shape()[extensionAxisIndex_];
    chunkShape_ = shape_;
    chunks_.resize(shape_.size());
    // The last dimension is implicitly created for the number of values in a field, i.e. there is no representation in
    // the axes. And the dimension of fields is never chunked I.e. fields are always returned whole.
    for (size_t index = 0; index < chunkShape_.size() - 1; ++index) {
        if (parts_[0].isAxisChunked(index)) {
            chunkShape_[index] = 1;
        }
        chunks_[index] = shape_[index] / chunkShape_[index] + ((shape_[index] % chunkShape_[index]) != 0);
    }
    chunks_.back() = 1;
    buffer_ = Buffer(chunkShape_);  // chunk_shape * field_entries
}

const std::vector<double>& ChunkedDataViewImpl::at(const std::vector<size_t>& chunkIndex) {

    auto idx(chunkIndex);

    for (const auto& part : parts_) {
        // Skip parts which the index isn't part of
        if (idx[extensionAxisIndex_] >= part.shape()[extensionAxisIndex_]) {
            idx[extensionAxisIndex_] -= part.shape()[extensionAxisIndex_];
            continue;
        }
        part.at(idx, buffer_);
        break;
    }

    return buffer_.values();
};

}  // namespace chunked_data_view
