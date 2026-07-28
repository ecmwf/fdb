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
#include <limits>
#include <memory>
#include <vector>

namespace chunked_data_view {
/// Concrete Extractor that retrieves GRIB fields from a real (or mock) FDB instance.
///
/// extractInto() converts the global bounding boxes into part-local and buffer-local
/// coordinate spaces, builds a narrowed MARS request via ViewPart::at(), inspects FDB,
/// and copies each returned field's float values into the correct slot of the output buffer.
class GribExtractor final : public Extractor {
public:

    explicit GribExtractor(const std::shared_ptr<FdbInterface> fdb);

    /// Sets the fill value written in place of bitmap-masked (missing) grid points.
    void setFillValue(float v) override { fillValue_ = v; }

    /// Retrieves one representative field to determine countValues and bytesPerValue.
    DataLayout layout(const metkit::mars::MarsRequest& mars_request) const override;

    /// Copies all fields in @p intersectionBoundingBox into @p ptr.
    /// @p chunkBoundingBox defines the origin for computing buffer offsets.
    size_t extractInto(const ViewPart& part, const ChunkedDataViewPartBoundingBox& chunkBoundingBox,
                       const ChunkedDataViewPartBoundingBox& intersectionBoundingBox, float* ptr,
                       size_t len) const override;

private:  // types

    /// Bundles all index-mapping and field metadata needed by writeInto() for one call.
    /// All members are references; the struct must not outlive the extractInto() call frame.
    struct WriteContext {
        const std::vector<Axis>& axes;
        const DataLayout& layout;
        const std::vector<size_t>& partAxisOffset;  ///< Intersection start in part-local axis space.
        const std::vector<size_t>& bufferOffset;    ///< Intersection start in chunk-buffer space.
        const std::vector<size_t>& bufferExtent;    ///< Per-axis size of the chunk buffer.
    };

private:  // members

    std::shared_ptr<FdbInterface> fdb_;
    float fillValue_ = std::numeric_limits<float>::quiet_NaN();

private:  // methods

    /// Iterates over @p list_iterator, maps each field's key to a buffer slot via
    /// computeBufferIndex(), and copies the GRIB float values into that slot.
    size_t writeInto(std::unique_ptr<ListIteratorInterface> list_iterator, const WriteContext& ctx, float* ptr,
                     size_t len) const;
};
}  // namespace chunked_data_view
