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
    /// inside build() after the MARS request string has been parsed. The builder takes
    /// exclusive ownership of the definition.
    ChunkedDataViewBuilder& addPart(std::string marsRequestKeyValues, std::vector<AxisDefinition> axes,
                                    std::unique_ptr<ExtractorDefinition> definition);

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

    /// Returns the FDB config path supplied at construction, if any.
    std::optional<std::filesystem::path> getFdbConfigPath() const { return configPath_; }

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
};
}  // namespace chunked_data_view
