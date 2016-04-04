/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/exception/Exceptions.h"
#include "eckit/config/Resource.h"

#include "fdb5/TocSchema.h"

namespace fdb {

//----------------------------------------------------------------------------------------------------------------------

TocSchema::TocSchema(const Key& dbKey) :
    dbKey_(dbKey)
{
}

TocSchema::~TocSchema()
{
}

bool TocSchema::matchTOC(const Key& key) const
{
    return key.match(dbKey_);
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb
