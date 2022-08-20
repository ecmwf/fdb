/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <stdlib.h>

#include <lustre/lustreapi.h>

/// Silence messages from the lustre library
void fdb5_lustreapi_silence_msg() {
    llapi_msg_set_level(LLAPI_MSG_OFF);
}

/// Provides a interface to Lustre API, however it must be accessed via a C code due to some
/// lustreapi.h incompatibility with C++
/// This code won't be compiled if build system doesnt HAVE_LUSTRE

int fdb5_lustreapi_file_create(const char* path, size_t stripesize, size_t stripecount) {
    return llapi_file_create(path, stripesize, -1, stripecount, LOV_PATTERN_RAID0);
}

/*
    int llapi_file_create(const char *name, unsigned long long stripe_size,
                            int stripe_offset, int stripe_count,
                            int stripe_pattern);

DESCRIPTION
       llapi_file_create() call is equivalent to llapi_file_open call with flags
       equal to O_CREAT|O_WRONLY and mode equal to 0644, followed by file close.

       stripe_size    specifies stripe size in bytes and should be multiple of
                      64 KiB not exceeding 4 GiB.

       stripe_offset  specifies an OST index from which the file should start,
                      -1 to use the default setting.

       stripe_count   specifies number of OSTs to stripe the file across, -1 to
                      use the default setting.

       stripe_pattern specifies striping pattern, only LOV_PATTERN_RAID0 is
                      available in this Lustre version, 0 to use the default
                      setting.
*/
