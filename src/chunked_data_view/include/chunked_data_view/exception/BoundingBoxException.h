// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "eckit/exception/Exceptions.h"

namespace chunked_data_view {

/// Thrown by BoundingBox when a construction or arithmetic operation produces an invalid box
/// (e.g. lower corner exceeds the upper corner after subtraction).
class BoundingBoxException : public eckit::Exception {

public:

    BoundingBoxException(const std::string&);
    BoundingBoxException(const std::string&, const eckit::CodeLocation&);
};
}  // namespace chunked_data_view
