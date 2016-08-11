/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/config/MasterConfig.h"

#include "eckit/runtime/Context.h"
#include "eckit/config/ResourceMgr.h"
#include "eckit/config/Resource.h"


#include "eckit/thread/AutoLock.h"
#include "eckit/thread/Mutex.h"

namespace fdb5 {

// static eckit::Mutex local_mutex;

//----------------------------------------------------------------------------------------------------------------------

MasterConfig::MasterConfig() {
    char *home1 = ::getenv("DHSHOME");
    if (home1) {
        eckit::Context::instance().home(home1);
    }

    char *home2 = ::getenv("FDB_HOME");
    if (home2) {
        eckit::Context::instance().home(home2);
    }

    if (!!home1 == !!home2) {
        throw eckit::SeriousBug("Either FDB_HOME or DHSHOME environment variable must be defined (but not both)");
    }

    eckit::ResourceMgr::instance().appendConfig("~/etc/config/fdb");

    schema_.load( schemaPath() );
}

MasterConfig::~MasterConfig() {
}

std::string MasterConfig::schemaPath() const {
    // eckit::AutoLock<eckit::Mutex> lock(local_mutex);
    static eckit::PathName fdbRules = eckit::Resource<eckit::PathName>("fdbRules", "~/etc/fdb/schema");
    return fdbRules;
}

const Schema &MasterConfig::schema() const {
    return schema_;
}

MasterConfig &MasterConfig::instance() {
    // eckit::AutoLock<eckit::Mutex> lock(local_mutex);
    static MasterConfig master;
    return master;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
