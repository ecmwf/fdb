/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/daos/DaosException.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosException::DaosException(const std::string& w) : Exception(w) {}

DaosException::DaosException(const std::string& w, const eckit::CodeLocation& l) : Exception(w, l) {}

DaosEntityNotFoundException::DaosEntityNotFoundException(const std::string& w) : DaosException(w) {}

DaosEntityNotFoundException::DaosEntityNotFoundException(const std::string& w, const eckit::CodeLocation& l) :
    DaosException(w, l) {}

DaosEntityAlreadyExistsException::DaosEntityAlreadyExistsException(const std::string& w) : DaosException(w) {}

DaosEntityAlreadyExistsException::DaosEntityAlreadyExistsException(const std::string& w, const eckit::CodeLocation& l) :
    DaosException(w, l) {}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
