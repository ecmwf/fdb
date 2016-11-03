/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Nov 2016

#include <algorithm>
#include <string>

#include "fdb5/LibFdb.h"

#include "mars_server_version.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

static LibFdb libfdb;

LibFdb::LibFdb() : Library("fdb") {}

const LibFdb& LibFdb::instance()
{
    return libfdb;
}

const void* LibFdb::addr() const { return this; }

std::string LibFdb::version() const { return mars_server_version_str(); }

std::string LibFdb::gitsha1(unsigned int count) const {
    std::string sha1(mars_server_git_sha1());
    if(sha1.empty()) {
        return "not available";
    }

    return sha1.substr(0,std::min(count,40u));
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

