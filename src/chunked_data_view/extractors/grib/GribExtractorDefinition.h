// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "chunked_data_view/Extractor.h"

#include <filesystem>
#include <limits>
#include <memory>
#include <optional>

namespace chunked_data_view {

/// ExtractorDefinition for full-field GRIB extraction via FDB.
///
/// buildExtractor() creates a FdbInterface and GribExtractor for the given MARS request,
/// applying the configured fill value.
class GribExtractorDefinition : public ExtractorDefinition {
public:
    /// FDB config path. std::nullopt uses the environment default (FDB5_CONFIG / FDB_HOME).
    std::optional<std::filesystem::path> fdbConfig;
    /// Fill value written for bitmap-masked (missing) grid points. Default: NaN.
    float fillValue = std::numeric_limits<float>::quiet_NaN();

    std::unique_ptr<Extractor> buildExtractor(const metkit::mars::MarsRequest& request) const override;
};

}  // namespace chunked_data_view
