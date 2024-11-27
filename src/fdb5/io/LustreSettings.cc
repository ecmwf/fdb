/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/io/LustreSettings.h"

#include "eckit/config/Resource.h"

#include "fdb5/fdb5_config.h"
#include "fdb5/LibFdb5.h"

#define LL_SUPER_MAGIC  0x0BD00BD0

#if defined(fdb5_HAVE_LUSTRE)
#include <sys/vfs.h>

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

int fdb5LustreapiFileCreate(eckit::PathName path, LustreStripe stripe) {

#if defined(fdb5_HAVE_LUSTRE)

    struct statfs buf;

    statfs(path.dirName().localPath(), &buf);
    if (buf.type == LL_SUPER_MAGIC) {

        static bool lustreapi_silence = false;

        if(not lustreapi_silence) {
            fdb5_lustreapi_silence_msg();
            lustreapi_silence = true;
        }

        return fdb5_lustreapi_file_create(path.localPath(), stripe.size_, stripe.count_);
    }
#endif

    /// @note since fdb5LustreapiSupported() should be guarding all calls to this function,
    ///       the code below should never be executed

    ASSERT_MSG(fdb5LustreapiSupported(), "fdb5LustreapiFileCreate() called yet fdb5_HAVE_LUSTRE is not defined");

    return 0; /* should never be executed */
}

bool stripeLustre() {
    static bool handleLustreStripe = eckit::Resource<bool>("fdbHandleLustreStripe;$FDB_HANDLE_LUSTRE_STRIPE", true);
    return fdb5LustreapiSupported() && handleLustreStripe;
}

LustreStripe stripeIndexLustreSettings() {

    static unsigned int fdbIndexLustreStripeCount = eckit::Resource<unsigned int>("fdbIndexLustreStripeCount;$FDB_INDEX_LUSTRE_STRIPE_COUNT", 1);
    static size_t fdbIndexLustreStripeSize = eckit::Resource<size_t>("fdbIndexLustreStripeSize;$FDB_INDEX_LUSTRE_STRIPE_SIZE", 8*1024*1024);

    return LustreStripe(fdbIndexLustreStripeCount, fdbIndexLustreStripeSize);
}


LustreStripe stripeDataLustreSettings() {

    static unsigned int fdbDataLustreStripeCount = eckit::Resource<unsigned int>("fdbDataLustreStripeCount;$FDB_DATA_LUSTRE_STRIPE_COUNT", 8);
    static size_t fdbDataLustreStripeSize = eckit::Resource<size_t>("fdbDataLustreStripeSize;$FDB_DATA_LUSTRE_STRIPE_SIZE", 8*1024*1024);

    return LustreStripe(fdbDataLustreStripeCount, fdbDataLustreStripeSize);
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
