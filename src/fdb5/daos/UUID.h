/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

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