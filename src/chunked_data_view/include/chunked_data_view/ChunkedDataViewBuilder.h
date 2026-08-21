// SPDX-FileCopyrightText: 2025 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "chunked_data_view/AxisDefinition.h"
#include "chunked_data_view/ChunkedDataView.h"
#include "chunked_data_view/Extractor.h"

#include <cstddef>
#include <filesystem>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <vector>


namespace chunked_data_view {

class ViewPart;

/// Fluent builder for constructing a ChunkedDataView from one or more MARS data parts.
///
/// Usage pattern:
///   1. Construct the builder (optionally supplying an FDB config path).
///   2. Call addPart() once per logical data region (e.g. once for surface fields, once for
///      pressure-level fields).
///   3. If more than one part is added, call extendOnAxis() to specify which axis stitches the
///      parts together.
///   4. Optionally call fillMissingValue() to set the sentinel for grid positions with no data.
///   5. Call build() to validate and assemble the ChunkedDataView.
class ChunkedDataViewBuilder {
public:

    /// @param configPath  Optional path to an FDB config file. Passed through to the FDB
    ///                    instance created when building with ExtractorType::Grib.
    explicit ChunkedDataViewBuilder(const std::optional<std::filesystem::path>& configPath = std::nullopt);


    // Deleted to make the ChunkedDataViewBuilder non-copyable. This is also needed by pybind11 layer.
    ChunkedDataViewBuilder(const ChunkedDataViewBuilder&) = delete;
    ChunkedDataViewBuilder& operator=(const ChunkedDataViewBuilder&) = delete;

    // declaring the deleted copy suppresses implicit moves, so restore them:
    ChunkedDataViewBuilder(ChunkedDataViewBuilder&&) = default;
    ChunkedDataViewBuilder& operator=(ChunkedDataViewBuilder&&) = default;

    /// Registers one data region (part) of the view.
    ///
    /// Each keyword in @p marsRequestKeyValues that has more than one value must appear in
    /// exactly one AxisDefinition in @p axes. Multiple keywords may share one axis
    /// (they form a compound Cartesian-product axis).
    ///
    /// Multiple parts can cover different variable types (e.g. surface and pressure-level
    /// fields) and are stitched together along the extension axis specified by extendOnAxis().
    ///
    /// @p definition is an ExtractorDefinition whose buildExtractor() is called once per part
    /// inside build() after the MARS request string has been parsed.
    ///
    /// The builder stores a *copy* of @p definition, so the caller keeps ownership of its
    /// object and may register the same configuration on several parts, or on several
    /// builders: the per-part defaults the builder applies (see setDefaultIfUnset) are written
    /// to the copy only.
    ChunkedDataViewBuilder& addPart(std::string marsRequestKeyValues, std::vector<AxisDefinition> axes,
                                    const ExtractorDefinition& definition);

    /// Sets the axis index along which multiple parts are concatenated.
    ///
    /// All parts must have identical extents on every axis except this one.
    /// For a single-part view this call is optional and the value is ignored.
    ///
    /// @throws eckit::UserError if @p index exceeds the number of axes in the first part.
    ChunkedDataViewBuilder& extendOnAxis(size_t index);

    /// Sets the fill value written to array positions not covered by any part.
    /// Defaults to NaN.
    ChunkedDataViewBuilder& fillMissingValue(float fillValue);

    /// Validates all parts, checks axis compatibility, and returns the assembled view.
    /// @throws eckit::UserError on misconfiguration (missing parts, axis mismatch, etc.).
    std::unique_ptr<ChunkedDataView> build();

private:

    std::optional<std::filesystem::path> configPath_{};
    std::vector<std::tuple<std::string, std::vector<AxisDefinition>, std::unique_ptr<ExtractorDefinition>>> parts_{};
    std::optional<size_t> extensionAxisIndex_ = std::nullopt;
    float fillValue_ = std::numeric_limits<float>::quiet_NaN();

    bool doPartsAlign(const std::vector<std::pair<ViewPart, std::unique_ptr<Extractor>>>& viewParts);

    /// Returns true if all parts use the same chunk size on every axis that is not
    /// WholeAxisChunking.
    ///
    /// For FixedSizeChunking axes the chunk size must be identical across all parts so
    /// that part boundaries coincide with Zarr chunk boundaries.  WholeAxisChunking axes
    /// (isSingleGrowingChunk=true) have one chunk per part whose size grows with the
    /// combined extent, so differing sizes are expected and exempt from this check.
    /// SingleValueChunking always produces chunk size 1 and is trivially consistent.
    ///
    /// Note: extension (adding parts) is supported for all three chunking types.
    static bool chunkingConsistencyCheck(const std::vector<std::pair<ViewPart, std::unique_ptr<Extractor>>>& viewParts);

    /// Throws unless every part agrees about the implicit (grid-point) dimension.
    ///
    /// That dimension is never the extension axis, so — like every other non-extension axis —
    /// all parts must match on it. Unlike the others it is not derived from the AxisDefinitions
    /// but from each extractor's DataLayout, and ChunkedDataViewImpl takes both of its values
    /// from the first part alone:
    ///   - countValues      the array's last extent. Differing grids have no representation in
    ///                      one zarr array (the last dimension would have to be ragged).
    ///   - countChunkValues the chunk's last extent. Differing field chunking means a part
    ///                      writes a differently sized block than the buffer is laid out for.
    ///
    /// Both are unchecked anywhere else, and both fail *silently* at read time: each extractor
    /// sizes its writes from its own layout, so a part with a larger field overruns its slots
    /// and still reports the expected message count.
    ///
    /// @throws eckit::UserError naming the offending part and both values.
    static void validateLayouts(const std::vector<std::pair<ViewPart, std::unique_ptr<Extractor>>>& viewParts);
};
}  // namespace chunked_data_view
