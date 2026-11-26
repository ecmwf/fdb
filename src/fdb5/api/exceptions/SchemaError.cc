// SPDX-FileCopyrightText: 2026- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/api/exceptions/SchemaError.h"

namespace fdb5 {

SchemaError::SchemaError(const std::string& path, const std::string& what, size_t line) :
    eckit::StreamParser::Error(path + ": " + what, line) {}

}  // namespace fdb5
