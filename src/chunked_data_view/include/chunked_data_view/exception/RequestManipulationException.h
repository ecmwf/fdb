// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "eckit/exception/Exceptions.h"

namespace chunked_data_view {

/// Thrown by RequestManipulation when a bounding box cannot be mapped to a single MARS request
/// (e.g. the box is not aligned with a sub-axis boundary, which would require multiple requests).
class RequestManipulationException : public eckit::Exception {

public:

    RequestManipulationException(const std::string&);
    RequestManipulationException(const std::string&, const eckit::CodeLocation&);
};
}  // namespace chunked_data_view
