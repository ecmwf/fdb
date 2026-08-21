// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include "eckit/exception/Exceptions.h"

namespace chunked_data_view {

/// Thrown by GribJumpExtractor when field retrieval fails (e.g. the FDB iterator returns no
/// results for a request, or a GribJump extraction result does not match the expected layout).
class GribJumpExtractorException : public eckit::Exception {

public:

    GribJumpExtractorException(const std::string&);
    GribJumpExtractorException(const std::string&, const eckit::CodeLocation&);
};
}  // namespace chunked_data_view
