// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#include "chunked_data_view/extractors/grib/GribExtractorDefinition.h"

#include "chunked_data_view/Fdb.h"
#include "chunked_data_view/extractors/grib/GribExtractor.h"

#include <memory>

namespace chunked_data_view {

std::unique_ptr<Extractor> GribExtractorDefinition::buildExtractor(
    const metkit::mars::MarsRequest& request) const {
    auto fdb = makeFdb(fdbConfig);
    auto ext = std::make_unique<GribExtractor>(std::move(fdb), request);
    ext->setFillValue(fillValue);
    return ext;
}

}  // namespace chunked_data_view
