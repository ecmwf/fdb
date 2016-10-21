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

#include <lustre/lustreapi.h>

int fdb5_lustreapi_file_create(const char* path, size_t stripesize, size_t stripecount) {

    return llapi_file_create(path, stripesize, 0, stripecount, LOV_PATTERN_RAID0);
}
