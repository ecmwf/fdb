// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Nicolau Manubens
/// @date May 2024

#pragma once

#include <uuid/uuid.h>

#include <string>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class UUID {

public:  // methods

    UUID() : internal{0} {}

    UUID(const std::string&);

public:  // members

    uuid_t internal;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5