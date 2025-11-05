/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include "chunked_data_view/Extractor.h"

#include "GribExtractor.h"
#include "eckit/exception/Exceptions.h"

#include <memory>

namespace chunked_data_view {
std::unique_ptr<Extractor> makeExtractor(ExtractorType type) {
    switch (type) {
        case ExtractorType::GRIB:
            return std::make_unique<GribExtractor>();
        default:
            throw eckit::UserError("makeExtractor: Unknown extractor type specified");
    }
}
}  // namespace chunked_data_view
