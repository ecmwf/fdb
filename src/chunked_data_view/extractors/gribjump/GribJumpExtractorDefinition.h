// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "chunked_data_view/AxisDefinition.h"
#include "chunked_data_view/Extractor.h"

#include <filesystem>
#include <memory>
#include <optional>

namespace chunked_data_view {

/// ExtractorDefinition for partial-field extraction via GribJump.
///
/// buildExtractor() sets GRIBJUMP_CONFIG_FILE (if gribjumpConfig is set), creates
/// FdbInterface + GribJump, and constructs a GribJumpExtractor for the given MARS request.
class GribJumpExtractorDefinition : public ExtractorDefinition {
public:

    GribJumpExtractorDefinition() = default;
    GribJumpExtractorDefinition(const GribJumpExtractorDefinition&) = default;
    GribJumpExtractorDefinition& operator=(const GribJumpExtractorDefinition&) = default;
    GribJumpExtractorDefinition(GribJumpExtractorDefinition&&) = default;
    GribJumpExtractorDefinition& operator=(GribJumpExtractorDefinition&&) = default;

    /// FDB config path. std::nullopt uses the environment default (FDB5_CONFIG / FDB_HOME).
    std::optional<std::filesystem::path> fdbConfig;
    /// GribJump config path. std::nullopt reads the GRIBJUMP_CONFIG_FILE env var.
    std::optional<std::filesystem::path> gribjumpConfig;
    /// Chunking strategy for the implicit (grid-point) dimension.
    /// Defaults to WholeAxisChunking (single chunk covering the full field/window).
    AxisDefinition::ChunkingType fieldChunking{AxisDefinition::WholeAxisChunking{}};

    void setDefaultIfUnset(const std::optional<std::filesystem::path>& fdbConfigPath) override;

    std::unique_ptr<ExtractorDefinition> copy() const override;

    std::unique_ptr<Extractor> buildExtractor(const metkit::mars::MarsRequest& request) const override;
};

}  // namespace chunked_data_view
