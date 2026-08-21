// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#include "chunked_data_view/extractors/gribjump/GribJumpExtractorDefinition.h"

#include "chunked_data_view/Fdb.h"
#include "chunked_data_view/extractors/gribjump/GribJumpExtractor.h"
#include "gribjump/GribJump.h"

#include <cstdlib>
#include <memory>

namespace chunked_data_view {

std::unique_ptr<Extractor> GribJumpExtractorDefinition::buildExtractor(const metkit::mars::MarsRequest& request) const {
    if (gribjumpConfig) {
        ::setenv("GRIBJUMP_CONFIG_FILE", gribjumpConfig->c_str(), /*overwrite=*/1);
    }
    auto fdb = makeFdb(fdbConfig);
    auto gj  = std::make_unique<gribjump::GribJump>();
    auto ext = std::make_unique<GribJumpExtractor>(std::move(fdb), std::move(gj), request, fieldChunking);
    ext->setFillValue(fillValue);
    return ext;
}

}  // namespace chunked_data_view
