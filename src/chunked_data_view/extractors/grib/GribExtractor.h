// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "chunked_data_view/Axis.h"
#include "chunked_data_view/DataLayout.h"
#include "chunked_data_view/Extractor.h"
#include "chunked_data_view/Fdb.h"
#include "chunked_data_view/ListIterator.h"
#include "chunked_data_view/Types.h"
#include "chunked_data_view/ViewPart.h"

#include "metkit/mars/MarsRequest.h"

#include <cstddef>
#include <limits>
#include <memory>
#include <mutex>
#include <vector>

namespace chunked_data_view {
/// Concrete Extractor that retrieves GRIB fields from a real (or mock) FDB instance.
///
/// extractInto() converts the global bounding boxes into part-local and buffer-local
/// coordinate spaces, builds a narrowed MARS request via ViewPart::at(), inspects FDB,
/// and copies each returned field's float values into the correct slot of the output buffer.
class GribExtractor final : public Extractor {
public:

    /// Constructs a GribExtractor and eagerly determines the DataLayout by retrieving
    /// a representative field from FDB for the given MARS request.
    explicit GribExtractor(std::unique_ptr<FdbInterface> fdb, const metkit::mars::MarsRequest& marsRequest);

    /// Sets the fill value written in place of bitmap-masked (missing) grid points.
    void setFillValue(float v) override { fillValue_ = v; }

    /// Copies all fields in @p intersectionBoundingBox into @p ptr.
    /// @p chunkBoundingBox defines the origin for computing buffer offsets.
    size_t extractInto(const ViewPart& part, const ChunkBoundingBox& chunkBoundingBox,
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

    std::unique_ptr<FdbInterface> fdb_;
    float fillValue_ = std::numeric_limits<float>::quiet_NaN();

    /// Serialises extractInto(). fdb_ is shared mutable backend state, but extractInto() is
    /// const and the pybind11 layer releases the GIL around it, so a threaded zarr consumer
    /// (e.g. dask) can enter it concurrently on one view. Reads serialise within a part;
    /// separate parts own separate extractors and still proceed in parallel.
    mutable std::mutex mutex_;

private:  // methods

    /// Iterates over @p list_iterator, maps each field's key to a buffer slot via
    /// computeBufferIndex(), and copies the GRIB float values into that slot.
    size_t writeInto(std::unique_ptr<ListIteratorInterface> list_iterator, const WriteContext& ctx, float* ptr,
                     size_t len) const;
};
}  // namespace chunked_data_view
