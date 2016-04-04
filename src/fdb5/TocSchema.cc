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

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocSchema::TocSchema(const Key& dbKey) :
    dbKey_(dbKey)
{
    indexType_ = eckit::Resource<std::string>( "fdbIndexType", "BTreeIndex" );
}

TocSchema::~TocSchema()
{
}

bool TocSchema::matchTOC(const Key& tocKey) const
{
    return tocKey.match(dbKey_);
}

eckit::PathName TocSchema::tocPath() const
{
    NOTIMP;
}

std::string TocSchema::dataFilePrefix(const Key& userKey) const
{
    NOTIMP;
}

eckit::PathName TocSchema::generateIndexPath(const Key& userKey) const
{
    NOTIMP;
}

eckit::PathName TocSchema::generateDataPath(const Key& userKey) const
{
    NOTIMP;
}

std::string TocSchema::tocEntry(const Key& userKey) const
{
    NOTIMP;
}

Index::Key TocSchema::dataIdx(const Key& userKey) const
{
    NOTIMP;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
