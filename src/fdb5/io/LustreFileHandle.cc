/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "mars_server_config.h"

#include <stdlib.h>

#include <fstream>

#include "eckit/log/Log.h"

#include "fdb5/LibFdb.h"

#if defined(LUSTREAPI_FOUND)
extern "C" {
#include <lustre/lustreapi.h>
}
#endif

namespace fdb5 {

int fdb5_lustreapi_file_create(const char* path, size_t stripesize, size_t stripecount) {

#if defined(LUSTREAPI_FOUND)
    return llapi_file_create(path, stripesize, 0, stripecount, LOV_PATTERN_RAID0);
#endif

    eckit::Log::warning() << "LustreAPI was not found on this system. Creating file with striped information reverts to default system behavior." << std::endl;

    // will not create here, since openForAppend will call correct handle

    return 0;
}

} // namespace fdb5
