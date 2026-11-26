// SPDX-FileCopyrightText: 2026- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>
#include <string>

#include "eckit/parser/StreamParser.h"

namespace fdb5 {

class SchemaError : public eckit::StreamParser::Error {
public:

    SchemaError(const std::string& path, const std::string& what, size_t line = 0);
};

}  // namespace fdb5
