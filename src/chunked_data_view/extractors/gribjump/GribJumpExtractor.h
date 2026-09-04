// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "chunked_data_view/Axis.h"
#include "chunked_data_view/DataLayout.h"
#include "chunked_data_view/Extractor.h"
#include "chunked_data_view/Fdb.h"
#include "chunked_data_view/ViewPart.h"

#include "gribjump/Types.h"  // gribjump::Range
#include "metkit/mars/MarsRequest.h"

#include <cstddef>
#include <limits>
#include <memory>
#include <mutex>
#include <vector>

namespace gribjump {
class GribJump;
class ExtractionIterator;
}  // namespace gribjump

namespace fdb5 {
class Key;
}  // namespace fdb5

namespace chunked_data_view {

// ---------------------------------------------------------------------------
// GribJumpExtractor
// ---------------------------------------------------------------------------

/// Concrete Extractor that uses GribJump as the data-retrieval backend.
///
/// Field enumeration (key ordering for computeBufferIndex) still uses
/// FdbInterface::inspect(). Actual value extraction uses
/// gribjump::GribJump::extract(), avoiding full GRIB decode.
///
/// An optional fieldChunking controls how the implicit (grid-point) dimension
/// is sub-divided into Zarr chunks.
class GribJumpExtractor final : public Extractor {
public:

    /// Constructs a GribJumpExtractor and eagerly determines the DataLayout by retrieving
    /// a representative field from FDB for the given MARS request.
    explicit GribJumpExtractor(std::unique_ptr<FdbInterface> fdb, std::unique_ptr<gribjump::GribJump> gj,
                               const metkit::mars::MarsRequest& marsRequest,
                               AxisDefinition::ChunkingType fieldChunking = AxisDefinition::WholeAxisChunking{});

    void setFillValue(float v) override { fillValue_ = v; }

    /// Copies the GribJump-extracted values for all fields in @p intersectionBB
    /// into the output buffer. Derives the per-chunk grid-point range from the
    /// last dimension of @p chunkBB.
    size_t extractInto(const ViewPart& part, const ChunkBoundingBox& chunkBB,
                       const ChunkedDataViewPartBoundingBox& intersectionBB, float* ptr, size_t len) const override;

private:  // types

    /// Bundles all index-mapping and field metadata needed by writeInto().
    /// All members are references; the struct must not outlive extractInto().
    struct WriteContext {
        const std::vector<Axis>& axes;
        const DataLayout& layout;
        const std::vector<size_t>& partAxisOffset;  ///< Intersection start in part-local axis space.
        const std::vector<size_t>& bufferOffset;    ///< Intersection start in chunk-buffer space.
        const std::vector<size_t>& bufferExtent;    ///< Per-axis size of the chunk buffer.
    };

private:  // members

    std::unique_ptr<FdbInterface> fdb_;
    std::unique_ptr<gribjump::GribJump> gj_;
    float fillValue_ = std::numeric_limits<float>::quiet_NaN();
    std::string gridHash_;  // md5GridSection of the grid; required by gribjump extraction

    /// Serialises extractInto(). fdb_ and gj_ are shared mutable backend state, but
    /// extractInto() is const and the pybind11 layer releases the GIL around it, so a threaded
    /// zarr consumer (e.g. dask) can enter it concurrently on one view. Reads serialise within
    /// a part; separate parts own separate extractors and still proceed in parallel.
    mutable std::mutex mutex_;

    // NOTE: the grid-point window and the per-chunk size are fully described by layout_
    // (countValues / countChunkValues) from the base class, so they are deliberately not
    // duplicated here. extractInto() derives the range it needs from its chunk bounding box.

private:  // methods

    /// Iterate @p gj_it, map each result to the correct buffer slot, and write
    /// the double values (with GribJump bitmap) as float32.
    size_t writeInto(const std::vector<fdb5::Key>& gj_keys, gribjump::ExtractionIterator& gj_it,
                     const WriteContext& ctx, float* ptr, size_t len) const;
};

}  // namespace chunked_data_view
