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
    ///                    instance created when building with ExtractorType::GRIB.
    explicit ChunkedDataViewBuilder(const std::optional<std::filesystem::path>& configPath = std::nullopt);

    /// Registers one data region (part) of the view.
    ///
    /// Each keyword in @p marsRequestKeyValues that has more than one value must appear in
    /// exactly one AxisDefinition in @p axes. Multiple keywords may share one axis
    /// (they form a compound Cartesian-product axis).
    ///
    /// Multiple parts can cover different variable types (e.g. surface and pressure-level
    /// fields) and are stitched together along the extension axis specified by extendOnAxis().
    ///
    /// @p extractor is taken as a shared_ptr because the assembled ChunkedDataViewImpl pairs
    /// each ViewPart with its Extractor and multiple ViewParts may legally share one Extractor
    /// instance (e.g. when two parts draw from the same FDB store). Shared ownership avoids
    /// copying the (potentially stateful, non-copyable) extractor while guaranteeing its
    /// lifetime extends at least as long as the built ChunkedDataView.
    ChunkedDataViewBuilder& addPart(std::string marsRequestKeyValues, std::vector<AxisDefinition> axes,
                                    std::shared_ptr<Extractor> extractor);

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
    std::vector<std::tuple<std::string, std::vector<AxisDefinition>, std::shared_ptr<Extractor>>> parts_{};
    std::optional<size_t> extensionAxisIndex_ = std::nullopt;
    float fillValue_ = std::numeric_limits<float>::quiet_NaN();

    bool doPartsAlign(const std::vector<std::pair<ViewPart, std::shared_ptr<Extractor>>>& viewParts);

    /// Returns true if all parts use the same chunk size on every non-extensible axis.
    ///
    /// For FixedSizeChunking axes the chunk size must be identical across all parts so
    /// that part boundaries coincide with Zarr chunk boundaries.  WholeAxisChunking axes
    /// (extensible=true) grow when parts are stitched together and are therefore exempt
    /// from this check.  SingleValueChunking always produces chunk size 1 and is trivially
    /// consistent.
    static bool chunkingConsistencyCheck(const std::vector<std::pair<ViewPart, std::shared_ptr<Extractor>>>& viewParts);
};
}  // namespace chunked_data_view
