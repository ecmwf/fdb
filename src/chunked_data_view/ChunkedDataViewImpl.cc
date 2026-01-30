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
    parts_(std::move(parts)),
    extensionAxisIndex_(extensionAxisIndex),
    shape_(parts_[0].shape().ndim()),
    chunkShape_(shape_.ndim()),
    chunks_(shape_.ndim()) {

    if (extensionAxisIndex_ >= shape_.ndim()) {
        std::ostringstream ss;
        ss << "ChunkedDataViewImpl: Extension axis is not referring to a valid axis index. Possible axis are: 0-";
        ss << parts_[0].shape().ndim() - 1 << ". You're selection is: " << extensionAxisIndex << std::endl;
        throw eckit::UserError(ss.str());
    }

    for (size_t dim = 0; dim < shape_.ndim(); ++dim) {
        if (dim == extensionAxisIndex_) {
            shape_[dim] =
                std::accumulate(std::begin(parts_), std::end(parts_), 0,
                                [&dim](const auto& acc, const auto& item) { return acc + item.shape()[dim]; });
        }
        else {
            const auto refVal = parts_[0].shape()[dim];
            if (std::any_of(std::begin(parts_), std::end(parts_),
                            [&refVal, &dim](const auto& item) { return item.shape()[dim] != refVal; })) {
                // TODO(kkratz): Add shape information to error message
                throw eckit::UserError(
                    "ChunkedDataViewImpl: Axis size mismatch. All axis besides the extension axis have to match in "
                    "their extent.");
            }
            shape_[dim] = refVal;
        }
        const auto refChunking = parts_[0].isAxisChunked(dim);
        if (std::any_of(std::begin(parts_), std::end(parts_),
                        [&refChunking, &dim](const auto& item) { return item.isAxisChunked(dim) != refChunking; })) {
            // TODO(kkratz): Add per axis chunking information to error message
            throw eckit::UserError("All parts must use the same chunking on an extension axis");
        }
        chunkShape_[dim] = refChunking ? 1 : shape_[dim];
        chunks_[dim]     = (shape_[dim] + chunkShape_[dim] - 1) / chunkShape_[dim];
    }
}

void ChunkedDataViewImpl::at(const ChunkIndex& index, float* ptr, size_t len) {
    for (const auto& part : parts_) {
        part.at(index, ptr, len, chunkShape_);
    }
};

size_t ChunkedDataViewImpl::datumSize() const {
    return parts_[0].layout().countValues;
}

}  // namespace chunked_data_view
