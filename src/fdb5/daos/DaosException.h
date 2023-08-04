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
