// SPDX-FileCopyrightText: 2025 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace chunked_data_view {

/// Describes how one axis of the resulting N-dimensional view is formed from MARS keys
/// and how that axis is subdivided into Zarr chunks.
///
/// One or more MARS keywords are combined into a single axis (e.g. ["date", "time"] forms
/// a compound date-time axis whose size is the product of their individual value counts).
/// The chunking type then controls how that combined axis is split:
///   - WholeAxisChunking: the entire axis is one chunk (useful when always reading the full extent)
///   - SingleValueChunking: each element of the axis is its own chunk
///   - FixedSizeChunking: the axis is divided into fixed-size chunks of @p chunkSize elements
struct AxisDefinition {

    /// The whole axis is a single chunk; the Zarr chunk extent equals the axis size.
    struct WholeAxisChunking {};

    /// Each element of the axis occupies its own chunk; chunk extent is always 1.
    struct SingleValueChunking {};

    /// The axis is divided into chunks of a fixed size.
    /// @p chunkSize must evenly divide the combined axis size, or evenly divide the
    /// fastest-varying constituent key's value count.
    struct FixedSizeChunking {
        size_t chunkSize;
    };

    /// Discriminated union of the supported chunking strategies.
    using ChunkingType = std::variant<WholeAxisChunking, SingleValueChunking, FixedSizeChunking>;

    /// Ordered list of MARS keyword names whose values form this axis.
    /// All listed keywords must appear in the associated MARS request with more than one value.
    std::vector<std::string> keys{};

    /// Chunking strategy applied to this axis.
    ChunkingType chunking{};

    /// Optional label used as the zarr dimension name.
    std::optional<std::string> name = std::nullopt;
};

}  // namespace chunked_data_view
