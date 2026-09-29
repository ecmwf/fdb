/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/daos/UUID.h"

#include "eckit/exception/Exceptions.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

UUID::UUID(const std::string& uuid) {

    if (uuid_parse(uuid.c_str(), internal) != 0) {
        throw eckit::BadParameter("The provided string is not a uuid.");
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
