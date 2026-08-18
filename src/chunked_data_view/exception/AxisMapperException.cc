// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "chunked_data_view/exception/AxisMapperException.h"

namespace chunked_data_view {
AxisMapperException::AxisMapperException(const std::string& w) : Exception(w) {}

AxisMapperException::AxisMapperException(const std::string& w, const eckit::CodeLocation& l) : Exception(w, l) {}

}  // namespace chunked_data_view
