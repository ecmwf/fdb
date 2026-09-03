// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "eckit/exception/Exceptions.h"

namespace chunked_data_view {

/// Thrown by GribExtractor when field retrieval fails (e.g. the FDB iterator returns no
/// results for a request, or a field's value count does not match the expected DataLayout).
class GribExtractorException : public eckit::Exception {

public:

    GribExtractorException(const std::string&);
    GribExtractorException(const std::string&, const eckit::CodeLocation&);
};
}  // namespace chunked_data_view
