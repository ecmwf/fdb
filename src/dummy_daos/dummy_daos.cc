/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * @file   dummy_daos.cc
 * @author Nicolau Manubens
 * @date   Jun 2022
 */

#include "dummy_daos.h"

#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"

using eckit::PathName;

//----------------------------------------------------------------------------------------------------------------------

typedef struct daos_handle_internal_t {
    PathName path;
} daos_handle_internal_t;

//----------------------------------------------------------------------------------------------------------------------

const PathName& dummy_daos_root() {

    static PathName tmpdir    = eckit::Resource<PathName>("$TMPDIR", "/tmp");
    static PathName daos_root = eckit::Resource<PathName>("$DUMMY_DAOS_DATA_ROOT", tmpdir / "fdb5_dummy_daos");
    daos_root                 = daos_root.realName();
    return daos_root;
}

const PathName& dummy_daos_get_handle_path(daos_handle_t handle) {

    ASSERT(handle.impl);
    return handle.impl->path;
}

//----------------------------------------------------------------------------------------------------------------------