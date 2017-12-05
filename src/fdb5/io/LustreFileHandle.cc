/*
 * (C) Copyright 1996-2017 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "mars_server_config.h"

#include "eckit/log/Log.h"

#include "fdb5/LibFdb.h"

extern "C" {
int fdb5_lustreapi_file_create(const char* path, size_t stripesize, size_t stripecount);
}

namespace fdb5 {

bool fdb5LustreapiSupported() {
#if defined(LUSTREAPI_FOUND)
    return true;
#else
    return false;
#endif
}

int fdb5LustreapiFileCreate(const char* path, size_t stripesize, size_t stripecount) {

#if defined(LUSTREAPI_FOUND)
    return fdb5_lustreapi_file_create(path, stripesize, stripecount);
#endif

    eckit::Log::warning() << "LustreAPI was not found on this system. Creating file with striped information reverts to default system behavior." << std::endl;

    // will not create here, since openForAppend will call correct handle

    return 0;
}

} // namespace fdb5
