/*
 * (C) Copyright 1996-2017 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/config/MasterConfig.h"

#include "eckit/config/ResourceMgr.h"
#include "eckit/config/Resource.h"


#include "eckit/thread/AutoLock.h"
#include "eckit/thread/Mutex.h"

namespace fdb5 {

// static eckit::Mutex local_mutex;

//----------------------------------------------------------------------------------------------------------------------

MasterConfig::MasterConfig() {

    schema_.load( schemaPath() );
}

MasterConfig::~MasterConfig() {
}

std::string MasterConfig::schemaPath() const {
    // eckit::AutoLock<eckit::Mutex> lock(local_mutex);
    static eckit::PathName fdbSchemaFile = eckit::Resource<eckit::PathName>("fdbSchemaFile;$FDB_SCHEMA_FILE", "~fdb/etc/fdb/schema");
    return fdbSchemaFile;
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
