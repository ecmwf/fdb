/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

// #include <algorithm>
#include "fdb5/MasterConfig.h"
#include "eckit/runtime/Context.h"
#include "eckit/config/ResourceMgr.h"
#include "eckit/config/Resource.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

MasterConfig::MasterConfig()
{
    char* home = ::getenv("DHSHOME");
    if(home) {
        eckit::Context::instance().home(home);
    }

    eckit::ResourceMgr::instance().appendConfig("~/etc/config/fdb");

    schema_.load( schemaPath() );
}

MasterConfig::~MasterConfig()
{
}

std::string MasterConfig::schemaPath() const
{
    static eckit::PathName fdbRules = eckit::Resource<eckit::PathName>("fdbRules", "~/etc/fdb/schema");
    return fdbRules;
}

const Schema& MasterConfig::schema() const
{
    return schema_;
}

MasterConfig& MasterConfig::instance()
{
    static MasterConfig master;
    return master;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
