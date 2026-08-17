// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "eckit/exception/Exceptions.h"

namespace chunked_data_view {

/// Thrown by AxisMapper when an AxisDefinition is invalid or incompatible with the MARS request
/// (e.g. a requested chunk size that does not divide the axis extent).
class AxisMapperException : public eckit::Exception {

public:

    AxisMapperException(const std::string&);
    AxisMapperException(const std::string&, const eckit::CodeLocation&);
};
}  // namespace chunked_data_view
