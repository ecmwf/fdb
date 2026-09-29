// SPDX-FileCopyrightText: 2025 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include <cstddef>
#include <ostream>
#include <sstream>
#include <utility>
#include <vector>

#include "ChunkedDataViewImpl.h"

#include "chunked_data_view/ViewPart.h"
#include "eckit/exception/Exceptions.h"


namespace chunked_data_view {

namespace {}  // namespace
bool checkForEqualChunking(const std::vector<std::pair<ViewPart, std::shared_ptr<Extractor>>>& parts) {

    const auto reference_chunks = parts[0].first.chunks();

    for (const auto& [part, _] : parts) {
        for (size_t i = 0; i < part.axes().size(); ++i) {
            if (part.chunks()[i].representativeExtent() != reference_chunks[i].representativeExtent()) {
                // If the axis is extensible along this axis, skip as we are fetching
                if (!reference_chunks[i].isExtensible()) {
                    return false;
                }
            }
        }
    }

    return true;
}

std::vector<size_t> ChunkedDataViewImpl::chunkShape(
    const std::vector<std::pair<ViewPart, std::shared_ptr<Extractor>>>& parts) {
    const ViewPart& reference_part = (parts[0].first);

    std::vector<size_t> reference_extensions;

    for (size_t i = 0; i < reference_part.axes().size(); ++i) {
        reference_extensions.push_back(reference_part.chunks()[i].representativeExtent());
    }
    reference_extensions.push_back(reference_part.layout().countValues);  // Add the size of the fields

    // Check for merging in case of extension axis
    if (reference_part.isExtensible(extensionAxisIndex_)) {
        reference_extensions[extensionAxisIndex_] = 0;
        for (const auto& [part, extractor] : parts) {
            reference_extensions[extensionAxisIndex_] += part.extension()[extensionAxisIndex_];
        }
    }

    return reference_extensions;
}

ChunkedDataViewImpl::ChunkedDataViewImpl(std::vector<std::pair<ViewPart, std::shared_ptr<Extractor>>>& parts,
                                         float fillValue, size_t extensionAxisIndex) :
    parts_(std::move(parts)), extensionAxisIndex_(extensionAxisIndex), fillValue_(fillValue) {

    const auto& first_part = std::get<0>(parts_[0]);
    chunkedDataViewShape_ = first_part.extension();

    if (extensionAxisIndex_ >= first_part.extension().size()) {
        std::ostringstream ss;
        ss << "ChunkedDataViewImpl: Extension axis is not referring to a valid axis index. Possible axis are: 0-";
        ss << first_part.extension().size() - 1 << ". Your selection is: " << extensionAxisIndex << std::endl;
        throw eckit::UserError(ss.str());
    }

    {
        size_t extensionOnExtensionAxis = 0;

        for (const auto& [part, _] : parts_) {
            for (size_t idx = 0; idx < chunkedDataViewShape_.size(); ++idx) {
                if (idx == extensionAxisIndex_) {
                    extensionOnExtensionAxis += part.extension()[idx];
                }
                else if (chunkedDataViewShape_[idx] != part.extension()[idx]) {
                    throw eckit::UserError(
                        "ChunkedDataViewImpl: Axis size mismatch. All axis besides the extension axis have to match in "
                        "their extent.");
                }
            }
        }
        chunkedDataViewShape_[extensionAxisIndex_] = extensionOnExtensionAxis;
    }
    // Add the implicit dimension
    chunkedDataViewShape_.push_back(first_part.layout().countValues);

    if (!checkForEqualChunking(parts_)) {
        throw eckit::UserError("ChunkedDataViewImpl::constructor: view parts need to have same chunking extensions.");
    }

    chunkShape_ = chunkShape(parts_);
    chunks_ = std::vector<size_t>(chunkShape_.size(), 0);

    // The last dimension is implicitly created for the number of values in a field, i.e. there is no representation in
    // the axes. And the dimension of fields is never chunked I.e. fields are always returned whole.
    for (size_t index = 0; index < chunkShape_.size() - 1; ++index) {
        // Integer ceil
        chunks_[index] = chunkedDataViewShape_[index] / chunkShape_[index] +
                         ((chunkedDataViewShape_[index] % chunkShape_[index]) != 0);
    }
    chunks_.back() = 1;  // Make the implicit dimension always single chunked
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

    std::vector<size_t> chunkLower(chunkShape_.size() - 1, 0);
    std::vector<size_t> chunkUpper(chunkShape_.size() - 1, 0);

    for (size_t i = 0; i < chunkShape_.size() - 1; ++i) {
        chunkLower[i] = chunkShape_[i] * chunkIndex[i];
        chunkUpper[i] = chunkLower[i] + chunkShape_[i] - 1;
    }

    ChunkedDataViewPartBoundingBox chunkBoundingBox{chunkLower, chunkUpper};

    for (const auto& [part, extractor] : parts_) {
        const std::optional<ChunkedDataViewPartBoundingBox> intersectionBoundingBox =
            part.boundingBox().intersect(chunkBoundingBox);

        // Skip the part if it doesn't contribute to the buffer
        if (!intersectionBoundingBox.has_value()) {
            continue;
        }
        size_t expected_msg_count = intersectionBoundingBox.value().entries();

        auto written = extractor->extractInto(part, chunkBoundingBox, intersectionBoundingBox.value(), ptr, len);

        if (written != expected_msg_count) {
            const PartBoundingBox& partRelativeBoundingBox =
                intersectionBoundingBox.value().subtract(part.boundingBox().lower());
            std::ostringstream ss;
            ss << "ChunkedDataViewImpl::at: retrieved " << written << " of " << expected_msg_count
               << " expected fields in request." << part.at(partRelativeBoundingBox);
            throw eckit::UserError(ss.str());
        }
    }
}

}  // namespace chunked_data_view
