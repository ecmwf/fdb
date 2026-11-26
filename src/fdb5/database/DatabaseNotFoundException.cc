// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/database/DatabaseNotFoundException.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DatabaseNotFoundException::DatabaseNotFoundException(const std::string& w) : Exception(w) {}

DatabaseNotFoundException::DatabaseNotFoundException(const std::string& w, const eckit::CodeLocation& l) :
    Exception(w, l) {}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
