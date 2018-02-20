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

#include "fdb5/LibFdb.h"

#include "mars_server_version.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

REGISTER_LIBRARY(LibFdb);

LibFdb::LibFdb() : Library("fdb"),
   schema_(NULL)
{
}

const Schema& LibFdb::schema() const
{
    if(schema_) return *schema_;

    schema_ = new Schema(schemaPath());

    return *schema_;
}

std::string LibFdb::schemaPath() const
{
    static eckit::PathName fdbSchemaFile = eckit::Resource<eckit::PathName>("fdbSchemaFile;$FDB_SCHEMA_FILE", "~fdb/etc/fdb/schema");
    return fdbSchemaFile;
}

LibFdb& LibFdb::instance()
{
    static LibFdb libfdb;
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

