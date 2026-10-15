// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Nicolau Manubens
/// @date Dec 2022

#pragma once

#include "eckit/exception/Exceptions.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DaosException : public eckit::Exception {
public:

    DaosException(const std::string&);
    DaosException(const std::string&, const eckit::CodeLocation&);
};

//----------------------------------------------------------------------------------------------------------------------

class DaosEntityNotFoundException : public DaosException {
public:

    DaosEntityNotFoundException(const std::string&);
    DaosEntityNotFoundException(const std::string&, const eckit::CodeLocation&);
};

class DaosEntityAlreadyExistsException : public DaosException {
public:

    DaosEntityAlreadyExistsException(const std::string&);
    DaosEntityAlreadyExistsException(const std::string&, const eckit::CodeLocation&);
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
