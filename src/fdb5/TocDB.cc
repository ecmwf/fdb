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

TocDB::TocDB(const Key& dbKey) : DB(dbKey),
    schema_(dbKey)
{
}

TocDB::~TocDB()
{
    closeIndexes();
}

bool TocDB::match(const Key& key) const
{
    return schema_.matchTOC(key);
}

const Schema& TocDB::schema() const
{
    return schema_;
}

bool TocDB::open()
{
    Log::error() << "Open not implemented for " << *this << std::endl;
    NOTIMP;
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

void TocDB::close()
{
    Log::error() << "Close not implemented for " << *this << std::endl;
    NOTIMP;
}

Index* TocDB::getCachedIndex( const PathName& path ) const
{
    IndexStore::const_iterator itr = indexes_.find( path );
    if( itr != indexes_.end() )
        return itr->second;
    else
        return 0;
}

Index& TocDB::getIndex(const PathName& path) const
{
    Index* idx = getCachedIndex(path);
    if( !idx )
    {
        idx = openIndex( path );
        ASSERT(idx);
        indexes_[ path ] = idx;
    }
    return *idx;
}

void TocDB::closeIndexes()
{
    for( IndexStore::iterator itr = indexes_.begin(); itr != indexes_.end(); ++itr )
    {
        Index* idx = itr->second;
        if( idx )
        {
            delete idx;
            itr->second = 0;
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
