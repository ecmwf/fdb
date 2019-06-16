/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/config/UMask.h"

#include "eckit/runtime/Main.h"
#include "eckit/config/ResourceMgr.h"
#include "eckit/config/Resource.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

UMask::UMask(mode_t mode):
    save_(::umask(mode)) {
}

UMask::~UMask() {
    ::umask(save_);
}

mode_t UMask::defaultUMask() {
    static long fdbDefaultUMask = eckit::Resource<long>("fdbDefaultUMask", 0);
    return fdbDefaultUMask;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
