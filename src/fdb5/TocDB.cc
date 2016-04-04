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

#include "fdb5/TocDB.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocDB::TocDB(const Key& key) : DB(key),
    schema_(key)
{
}

TocDB::~TocDB()
{
}

bool TocDB::match(const Key& key) const
{
    return schema_.matchTOC(key);
}

const Schema& TocDB::schema() const
{
    return schema_;
}

void TocDB::archive(const Key& key, const void *data, Length length)
{
    Log::error() << "Archive not implemented for " << *this << std::endl;
    NOTIMP;
}

void TocDB::flush()
{
    Log::error() << "Flush not implemented for " << *this << std::endl;
    NOTIMP;
}

eckit::DataHandle* TocDB::retrieve(const MarsTask& task, const Key& key) const
{
    Log::error() << "Retrieve not implemented for " << *this << std::endl;
    NOTIMP;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
