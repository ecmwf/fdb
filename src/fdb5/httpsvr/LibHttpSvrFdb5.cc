/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/httpsvr/LibHttpSvrFdb5.h"

#include "fdb5/fdb5_config.h"
#include "fdb5/fdb5_version.h"

namespace fdb5 {
namespace httpsvr {

//----------------------------------------------------------------------------------------------------------------------

REGISTER_LIBRARY(LibHttpSvrFdb5);

LibHttpSvrFdb5::LibHttpSvrFdb5() : Plugin("fdb5-httpsvr", "fdb5-httpsvr-plugin") {}

LibHttpSvrFdb5& LibHttpSvrFdb5::instance() {
    static LibHttpSvrFdb5 lib;
    return lib;
}

const void* LibHttpSvrFdb5::addr() const {
    return this;
}

std::string LibHttpSvrFdb5::version() const {
    return fdb5_version_str();
}

std::string LibHttpSvrFdb5::gitsha1(unsigned int count) const {
    std::string sha1(fdb5_git_sha1());
    if (sha1.empty()) {
        return "not available";
    }

    return sha1.substr(0, std::min(count, 40u));
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace httpsvr
} // namespace fdb5
