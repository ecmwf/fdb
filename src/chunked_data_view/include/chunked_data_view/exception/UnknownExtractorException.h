// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "eckit/exception/Exceptions.h"

namespace chunked_data_view {

/// Thrown when an unrecognised ExtractorType is supplied to the factory.
class UnknownExtractorException : public eckit::Exception {

public:

    UnknownExtractorException(const std::string&);
    UnknownExtractorException(const std::string&, const eckit::CodeLocation&);
};
}  // namespace chunked_data_view
