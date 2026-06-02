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

#include "chunked_data_view/exception/GribExtractorException.h"

#include <cstddef>
#include <ostream>
#include <sstream>
#include <utility>
#include <vector>

namespace chunked_data_view {

namespace {

size_t countFieldsForPart(const ViewPart& part, const std::vector<size_t>& chunkShape,
                          const size_t extensionAxisIndex) {
    size_t result = 1;
    for (size_t i = 0; i < chunkShape.size() - 1; ++i) {
        if (i == extensionAxisIndex) {
            result *= part.shape()[i];
        }
        else {
            result *= chunkShape[i];
        }
    }
    return result;
}

}  // namespace

ChunkedDataViewImpl::ChunkedDataViewImpl(std::vector<std::pair<ViewPart, std::shared_ptr<Extractor>>> parts,
                                         size_t extensionAxisIndex) :
    parts_(std::move(parts)), extensionAxisIndex_(extensionAxisIndex) {

    const auto& first_part = std::get<0>(parts_[0]);
    shape_ = first_part.shape();

    for (const auto& [part, _] : parts_) {
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

    if (extensionAxisIndex_ >= first_part.shape().size() - 1) {  // The implicit dimension must be subtracted
        std::ostringstream ss;
        ss << "ChunkedDataViewImpl: Extension axis is not referring to a valid axis index. Possible axis are: 0-";
        ss << first_part.shape().size() - 2 << ". You're selection is: " << extensionAxisIndex << std::endl;
        throw eckit::UserError(ss.str());
    }

    shape_[extensionAxisIndex_] -= first_part.shape()[extensionAxisIndex_];
    chunkShape_ = shape_;
    chunks_.resize(shape_.size());
    // The last dimension is implicitly created for the number of values in a field, i.e. there is no representation in
    // the axes. And the dimension of fields is never chunked I.e. fields are always returned whole.
    for (size_t index = 0; index < chunkShape_.size() - 1; ++index) {
        if (first_part.isAxisChunked(index)) {
            chunkShape_[index] = 1;
        }
        chunks_[index] = shape_[index] / chunkShape_[index] + ((shape_[index] % chunkShape_[index]) != 0);
    }
    chunks_.back() = 1;
}

void ChunkedDataViewImpl::at(const std::vector<size_t>& chunkIndex, float* ptr, size_t len) {

    if (chunkIndex.size() != chunks_.size()) {
        std::ostringstream ss;
        ss << "ChunkedDataViewImpl::at: Expected chunk index of dimension " << chunks_.size() << ", got dimension "
           << chunkIndex.size();
        throw eckit::UserError(ss.str());
    }

    for (size_t i = 0; i < chunkIndex.size(); ++i) {
        if (chunkIndex[i] >= chunks_[i]) {
            std::ostringstream ss;
            ss << "ChunkedDataViewImpl::at: Chunk index out of bounds at dimension " << i << ". Index is "
               << chunkIndex[i] << " but number of chunks is " << chunks_[i];
            throw eckit::UserError(ss.str());
        }
    }

    const auto& first_part = std::get<0>(parts_[0]);

    if (!first_part.isAxisChunked(extensionAxisIndex_)) {
        // NoChunking: single chunk spans all parts on the extension axis.
        // Each part writes directly into the combined buffer. The extension axis
        // parameters tell computeBufferIndex to use the combined size for strides
        // and offset each part's indices on the extension axis.
        size_t totalExtSize = shape_[extensionAxisIndex_];
        size_t extOffset = 0;

        for (const auto& [part, extractor] : parts_) {

            size_t expected_msg_count = countFieldsForPart(part, chunkShape_, extensionAxisIndex_);

            try {
                auto written =
                    extractor->extractInto(part, chunkIndex, ptr, len, extensionAxisIndex_, totalExtSize, extOffset);

                if (written != expected_msg_count) {
                    std::ostringstream ss;
                    ss << "ViewPart::at: retrieved only " << written << " of " << expected_msg_count
                       << " fields in request." << part.at(chunkIndex);
                    throw eckit::UserError(ss.str());
                }
            }
            catch (GribExtractorException& exception) {
                std::ostringstream ss;
                ss << exception.what();
                ss << "Request was: " << part.at(chunkIndex) << std::endl;
                throw GribExtractorException(ss.str());
            }

            extOffset += part.shape()[extensionAxisIndex_];
        }
        return;
    }

    // IndividualChunking: route to the single part that owns this chunk index
    auto idx(chunkIndex);

    for (const auto& [part, extractor] : parts_) {
        if (idx[extensionAxisIndex_] >= part.shape()[extensionAxisIndex_]) {
            idx[extensionAxisIndex_] -= part.shape()[extensionAxisIndex_];
            continue;
        }
        extractor->extractInto(part, idx, ptr, len);
        return;
    }
    throw eckit::SeriousBug("ChunkedDataViewImpl::at - This code should never be reached.");
}

}  // namespace chunked_data_view
