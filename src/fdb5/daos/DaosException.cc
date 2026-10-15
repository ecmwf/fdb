// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

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
