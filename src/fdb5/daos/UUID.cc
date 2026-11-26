// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

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
