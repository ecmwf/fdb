/*
 * (C) Copyright 1996- ECMWF.
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

#include "eckit/config/Resource.h"
#include "eckit/config/YAMLConfiguration.h"
#include "eckit/log/Log.h"

#include "fdb5/LibFdb.h"
#include "fdb5/config/Config.h"

#include "fdb5/fdb_version.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

REGISTER_LIBRARY(LibFdb);

LibFdb::LibFdb() : Library("fdb") {}

LibFdb& LibFdb::instance()
{
    static LibFdb libfdb;
    return libfdb;
}

const Config& LibFdb::defaultConfig() {

    static bool initted = false;
    static Config config;

    if (!initted) {
        config = config.expandConfig();
        initted = true;
    }

    return config;
}

const void* LibFdb::addr() const { return this; }

std::string LibFdb::version() const { return fdb_version_str(); }

std::string LibFdb::gitsha1(unsigned int count) const {
    std::string sha1(fdb_git_sha1());
    if(sha1.empty()) {
        return "not available";
    }

    return sha1.substr(0,std::min(count,40u));
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

