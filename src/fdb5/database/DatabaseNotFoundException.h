// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Nicolau Manubens
/// @date May 2024

#pragma once

#include "eckit/exception/Exceptions.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DatabaseNotFoundException : public eckit::Exception {
public:

    DatabaseNotFoundException(const std::string&);
    DatabaseNotFoundException(const std::string&, const eckit::CodeLocation&);
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
