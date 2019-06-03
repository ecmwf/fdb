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

/// Provides a interface to Lustre API, however it must be accessed via a C code due to some
/// lustreapi.h incompatibility with C++
/// This code won't be compiled if build system doesnt HAVE_LUSTRE
int fdb5_lustreapi_file_create(const char* path, size_t stripesize, size_t stripecount) {
    return llapi_file_create(path, stripesize, -1, stripecount, LOV_PATTERN_RAID0);
}
