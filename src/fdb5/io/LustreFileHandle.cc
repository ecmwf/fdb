/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/fdb5_config.h"

#include "eckit/log/Log.h"
#include "eckit/exception/Exceptions.h"

#include "fdb5/LibFdb5.h"


#if defined(HAVE_LUSTRE)
#include <lustre/lustreapi.h>

extern "C" {
int fdb5_lustreapi_file_create(const char* path, size_t stripesize, size_t stripecount) {

    return llapi_file_create(path, stripesize, -1, stripecount, LOV_PATTERN_RAID0);
}
}
#endif

namespace fdb5 {

bool fdb5LustreapiSupported() {
#if defined(HAVE_LUSTRE)
    return true;
#else
    return false;
#endif
}

int fdb5LustreapiFileCreate(const char* path, size_t stripesize, size_t stripecount) {

#if defined(HAVE_LUSTRE)
    return fdb5_lustreapi_file_create(path, stripesize, stripecount);
#endif

    /// @note since fdb5LustreapiSupported() should be guarding all calls to this function,
    ///       the code below should never be executed

    ASSERT_MSG(fdb5LustreapiSupported(), "fdb5LustreapiFileCreate() called yet HAVE_LUSTRE is not defined");

    return 0; /* should never be executed */
}

} // namespace fdb5
