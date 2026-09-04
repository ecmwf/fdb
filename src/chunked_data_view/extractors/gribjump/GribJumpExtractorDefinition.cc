// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#include "chunked_data_view/extractors/gribjump/GribJumpExtractorDefinition.h"

#include "chunked_data_view/Fdb.h"
#ifdef HAVE_ZARR_GRIBJUMP_EXTRACTOR
#include "chunked_data_view/extractors/gribjump/GribJumpExtractor.h"
#include "gribjump/GribJump.h"
#endif

#include "eckit/exception/Exceptions.h"

#include <cstdlib>
#include <memory>

namespace chunked_data_view {

void GribJumpExtractorDefinition::setDefaultIfUnset(const std::optional<std::filesystem::path>& fdbConfigPath) {
    if (!fdbConfig.has_value()) {
        fdbConfig = fdbConfigPath;
    }
}

std::unique_ptr<ExtractorDefinition> GribJumpExtractorDefinition::copy() const {
    return std::make_unique<GribJumpExtractorDefinition>(*this);
}

std::unique_ptr<Extractor> GribJumpExtractorDefinition::buildExtractor(const metkit::mars::MarsRequest& request) const {
#ifndef HAVE_ZARR_GRIBJUMP_EXTRACTOR
    // This configuration type stays available in every build, so that user code does not depend
    // on how fdb was compiled. Only building the extractor fails, and it says why.
    throw eckit::UserError(
        "GribJumpExtractorDefinition: this build has no GribJump support. Rebuild fdb with "
        "-DENABLE_ZARR_GRIBJUMP_EXTRACTOR=ON (requires a bundle build providing gribjump).");
#else
    if (gribjumpConfig) {
        ::setenv("GRIBJUMP_CONFIG_FILE", gribjumpConfig->c_str(), /*overwrite=*/1);
    }
    auto fdb = makeFdb(fdbConfig);
    auto gj = std::make_unique<gribjump::GribJump>();
    auto ext = std::make_unique<GribJumpExtractor>(std::move(fdb), std::move(gj), request, fieldChunking);
    return ext;
#endif
}

}  // namespace chunked_data_view
