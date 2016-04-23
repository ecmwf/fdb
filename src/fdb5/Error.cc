/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/Error.h"
#include "fdb5/Schema.h"

namespace fdb5 {

Error::Error(const eckit::CodeLocation& loc, const std::string&s) : Exception( std::string("FDB::Error - ") + s, loc)
{
}

SchemaHasChanged::SchemaHasChanged(const Schema& schema):
    Exception("Schema has changed: " + schema.path()),
    path_(schema.path()) {
}

SchemaHasChanged::~SchemaHasChanged() throw() {

}
} // namespace fdb5
