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
#include "fdb5/io/LustreFileHandle.h"


#if defined(fdb5_HAVE_LUSTRE)
extern "C" {
void fdb5_lustreapi_silence_msg();
int  fdb5_lustreapi_file_create(const char* path, size_t stripesize, size_t stripecount);
}
#endif

//----------------------------------------------------------------------------------------------------------------------

namespace fdb5 {

bool fdb5LustreapiSupported() {
#if defined(fdb5_HAVE_LUSTRE)
    return true;
#else
    return false;
#endif
}

int fdb5LustreapiFileCreate(const char* path, size_t stripesize, size_t stripecount) {

#if defined(fdb5_HAVE_LUSTRE)

    static bool lustreapi_silence = false;

    if(not lustreapi_silence) {
        fdb5_lustreapi_silence_msg();
        lustreapi_silence = true;
    }

    return fdb5_lustreapi_file_create(path, stripesize, stripecount);

#endif

    /// @note since fdb5LustreapiSupported() should be guarding all calls to this function,
    ///       the code below should never be executed

    ASSERT_MSG(fdb5LustreapiSupported(), "fdb5LustreapiFileCreate() called yet fdb5_HAVE_LUSTRE is not defined");

    return 0; /* should never be executed */
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
