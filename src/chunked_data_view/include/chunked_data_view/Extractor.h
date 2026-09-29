// SPDX-FileCopyrightText: 2025 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "chunked_data_view/DataLayout.h"

#include "chunked_data_view/ViewPart.h"
#include "metkit/mars/MarsRequest.h"

#include <cstddef>

namespace eckit {
class DataHandle;
}

namespace chunked_data_view {

/// Abstract interface for retrieving field data into a Zarr chunk buffer.
///
/// Concrete implementations are typically stateful (e.g. they hold an open FDB handle or
/// an HTTP client) and non-copyable by design. For this reason the builder and the assembled
/// ChunkedDataViewImpl always hold extractors via std::shared_ptr: shared ownership lets
/// multiple ViewParts reference the same backing store without copying, and it guarantees
/// the extractor's lifetime is tied to the view rather than to any single part.
class Extractor {
public:

    virtual ~Extractor() = default;

    /// Retrieves one field for @p req and returns its layout (number of values and bytes per value).
    /// All fields in a part are expected to share the same layout; this method establishes it.
    virtual DataLayout layout(const metkit::mars::MarsRequest& req) const = 0;

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
    virtual size_t extractInto(const ViewPart& part, const ChunkedDataViewPartBoundingBox& chunkBoundingBox,
                               const ChunkedDataViewPartBoundingBox& intersectionBoundingBox, float* ptr,
                               size_t len) const = 0;
};

enum class ExtractorType {
    GRIB
};
}  // namespace chunked_data_view
