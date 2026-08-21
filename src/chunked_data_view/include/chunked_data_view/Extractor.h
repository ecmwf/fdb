// SPDX-FileCopyrightText: 2025 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "chunked_data_view/AxisDefinition.h"
#include "chunked_data_view/DataLayout.h"

#include "chunked_data_view/Types.h"
#include "chunked_data_view/ViewPart.h"

#include "metkit/mars/MarsRequest.h"

#include "chunked_data_view/exception/GribExtractorException.h"
#include "chunked_data_view/exception/GribJumpExtractorException.h"

#include <cstddef>
#include <filesystem>
#include <memory>
#include <optional>
#include <variant>

namespace eckit {
class DataHandle;
}

namespace chunked_data_view {

/// Abstract interface for retrieving field data into a Zarr chunk buffer.
///
/// Concrete implementations are typically stateful (e.g. they hold an open FDB handle or
/// an HTTP client) and non-copyable by design. For this reason the builder and the assembled
/// ChunkedDataViewImpl always hold extractors via std::unique_ptr: each ViewPart owns its
/// Extractor exclusively, and the extractor's lifetime is tied to the view.
class Extractor {
public:

    virtual ~Extractor() = default;

    /// Returns the DataLayout established by the concrete extractor at construction time.
    /// Describes the size of the implicit (grid-point) dimension and the per-chunk size.
    DataLayout layout() const { return layout_; }

    /// Sets the fill value used to replace GRIB bitmap missing-value sentinels.
    /// Default no-op; override in concrete extractors that read real field data.
    virtual void setFillValue(float) {}

    /// Copies the field values that fall inside @p intersectionBoundingBox into the output buffer.
    ///
    /// Both bounding boxes are expressed in the global ChunkedDataView index space.
    /// @p chunkBoundingBox covers the full Zarr chunk being filled; @p intersectionBoundingBox
    /// is the sub-region of that chunk owned by @p part (i.e. the intersection of the chunk with
    /// the part's bounding box, guaranteed non-empty by the caller).
    ///
    /// The caller must ensure that @p ptr points to a buffer of at least @p len floats.
    ///
    /// @param part                    the ViewPart to retrieve data from
    /// @param chunkBoundingBox        bounding box of the current chunk in global view coordinates
    /// @param intersectionBoundingBox non-empty intersection of the chunk with @p part's bounding box
    /// @param ptr                     output buffer to write field values into
    /// @param len                     capacity of the output buffer in number of floats
    /// @return number of GRIB messages written into the buffer
    virtual size_t extractInto(const ViewPart& part, const ChunkBoundingBox& chunkBoundingBox,
                               const ChunkedDataViewPartBoundingBox& intersectionBoundingBox, float* ptr,
                               size_t len) const = 0;

protected:

    /// Populated by each concrete extractor's constructor; returned by layout().
    DataLayout layout_{};
};


/// Abstract factory that produces a concrete Extractor for a given (already-parsed)
/// MARS request. ChunkedDataViewBuilder::build() calls buildExtractor() once per
/// registered part after it has parsed and validated the request string.
///
/// Concrete definitions (GribExtractorDefinition, GribJumpExtractorDefinition) carry
/// their own backend-specific configuration (FDB path, fill value, etc.) as public
/// data members. They are also the user-facing configuration objects: the Python layer
/// binds them as ExtractorType.Grib / ExtractorType.GribJump.
class ExtractorDefinition {
public:

    virtual ~ExtractorDefinition() = default;

    /// Adopts @p fdbConfigPath as this definition's FDB config unless one was set explicitly.
    ///
    /// Called by ChunkedDataViewBuilder::addPart() so that a definition which names no FDB
    /// config inherits the builder's, without the builder having to know which backend it
    /// is holding. An empty path on both sides leaves FDB to resolve its own configuration
    /// from the environment (FDB5_CONFIG / FDB_HOME).
    virtual void setDefaultIfUnset(const std::optional<std::filesystem::path>& fdbConfigPath) = 0;

    /// Returns an independent copy of this definition.
    ///
    /// ChunkedDataViewBuilder::addPart() stores a copy rather than the caller's object, so one
    /// configuration can be registered on several parts (and several builders) without the
    /// builder's own defaults leaking back into it.
    virtual std::unique_ptr<ExtractorDefinition> copy() const = 0;

    /// Construct the concrete Extractor for the given MARS request.
    /// Called exactly once per part by ChunkedDataViewBuilder::build().
    virtual std::unique_ptr<Extractor> buildExtractor(const metkit::mars::MarsRequest& request) const = 0;

protected:

    /// Definitions are plain, copyable configuration objects. ChunkedDataViewBuilder::addPart()
    /// takes ownership of the definition it is handed, so a caller that wants to reuse one
    /// configuration across several parts hands over a copy of its concrete definition.
    ///
    /// These members are protected rather than public so that a copy can only be made through
    /// a concrete definition; copying via an ExtractorDefinition& would slice.
    ExtractorDefinition() = default;
    ExtractorDefinition(const ExtractorDefinition&) = default;
    ExtractorDefinition& operator=(const ExtractorDefinition&) = default;
    ExtractorDefinition(ExtractorDefinition&&) = default;
    ExtractorDefinition& operator=(ExtractorDefinition&&) = default;
};

}  // namespace chunked_data_view
