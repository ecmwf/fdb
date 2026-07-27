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

#include "chunked_data_view/Axis.h"
#include "chunked_data_view/DataLayout.h"
#include "chunked_data_view/Extractor.h"
#include "chunked_data_view/Fdb.h"
#include "chunked_data_view/ListIterator.h"

#include "chunked_data_view/ViewPart.h"

#include <cstddef>
#include <memory>
#include <vector>

namespace chunked_data_view {
class GribExtractor final : public Extractor {
public:

    GribExtractor(const std::shared_ptr<FdbInterface> fdb);

    DataLayout layout(const metkit::mars::MarsRequest& mars_request) const override;


    size_t extractInto(const ViewPart& part, const ChunkedDataViewPartBoundingBox& chunkBoundingBox,
                       const ChunkedDataViewPartBoundingBox& intersectionBoundingBox, float* ptr,
                       size_t len) const override;

private:  // types

    /// Bundles all index-mapping and field metadata needed by writeInto.
    struct WriteContext {
        const std::vector<Axis>& axes;
        const DataLayout& layout;
        const std::vector<size_t>& partAxisOffset;  // Intersection start within the part's axis space
        const std::vector<size_t>& bufferOffset;    // Intersection start within the chunk buffer
        const std::vector<size_t>& bufferExtent;    // Total size of the chunk buffer per axis
    };

private:  // members

    std::shared_ptr<FdbInterface> fdb_;

private:  // methods

    size_t writeInto(std::unique_ptr<ListIteratorInterface> list_iterator, const WriteContext& ctx, float* ptr,
                     size_t len) const;
};
}  // namespace chunked_data_view
