/*
 * (C) Copyright 2026- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

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
