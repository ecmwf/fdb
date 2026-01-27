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

#include "chunked_data_view/ViewPart.h"
#include "eckit/exception/Exceptions.h"

#include <cstddef>
#include <ostream>
#include <sstream>
#include <utility>
#include <vector>

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
                throw eckit::UserError(
                    "ChunkedDataViewImpl: Axis size mismatch. All axis besides the extension axis have to match in "
                    "their extent.");
            }
        }
    }

    if (extensionAxisIndex_ >= parts_[0].shape().size() - 1) {  // The implicit dimension must be subtracted
        std::ostringstream ss;
        ss << "ChunkedDataViewImpl: Extension axis is not referring to a valid axis index. Possible axis are: 0-";
        ss << parts_[0].shape().size() - 2 << ". You're selection is: " << extensionAxisIndex << std::endl;
        throw eckit::UserError(ss.str());
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
}

void ChunkedDataViewImpl::at(const std::vector<size_t>& chunkIndex, float* ptr, size_t len) {

    auto idx(chunkIndex);

    // for (const auto& part : parts_) {
    //     // Skip parts which the index isn't part of
    //     if (idx[extensionAxisIndex_] >= part.shape()[extensionAxisIndex_]) {
    //         idx[extensionAxisIndex_] -= part.shape()[extensionAxisIndex_];
    //         continue;
    //     }
    //     part.at(idx, ptr, len, countFields());
    //     break;
    // }

    // Ensure ptr, len is large enough to hold output
    if (const auto expectedLen = countChunkValues(); len != expectedLen) {
        std::ostringstream out{};
        out << "Output buffer provided to ChunkedDataViewImpl::at(...) has the wrong size. "
            << "Expected: 'len = " << expectedLen << ", actual: 'len = " << len;
        throw eckit::UserError(out.str());
    }


    for (const auto& part : parts_) {
        part.at(idx, ptr, len);
    }
};

}  // namespace chunked_data_view
