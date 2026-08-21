// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "chunked_data_view/Extractor.h"

#include <filesystem>
#include <memory>
#include <optional>

namespace chunked_data_view {

/// ExtractorDefinition for full-field GRIB extraction via FDB.
///
/// buildExtractor() creates a FdbInterface and GribExtractor for the given MARS request,
/// applying the configured fill value.
class GribExtractorDefinition : public ExtractorDefinition {
public:

    GribExtractorDefinition() = default;
    GribExtractorDefinition(const GribExtractorDefinition&) = default;
    GribExtractorDefinition& operator=(const GribExtractorDefinition&) = default;
    GribExtractorDefinition(GribExtractorDefinition&&) = default;
    GribExtractorDefinition& operator=(GribExtractorDefinition&&) = default;

    /// FDB config path. std::nullopt uses the environment default (FDB5_CONFIG / FDB_HOME).
    std::optional<std::filesystem::path> fdbConfig;

    void setDefaultIfUnset(const std::optional<std::filesystem::path>& fdbConfigPath) override;

    std::unique_ptr<ExtractorDefinition> copy() const override;

    std::unique_ptr<Extractor> buildExtractor(const metkit::mars::MarsRequest& request) const override;
};

}  // namespace chunked_data_view
