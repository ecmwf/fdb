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
#include "eckit/thread/AutoLock.h"
#include "eckit/config/Resource.h"

#include "fdb5/TocActions.h"

#include "fdb5/Error.h"
#include "fdb5/TocDBReader.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocDBReader::TocDBReader(const Key& key) :
    TocDB(key),
    toc_(path_)
{
    Log::info() << "TocDBReader for TOC [" << path_ << "]" << std::endl;
}

TocDBReader::~TocDBReader()
{
}

bool TocDBReader::selectIndex(const Key& key)
{
    current_ = toc_.indexes(key);
    return (current_.size() != 0);
}

bool TocDBReader::open() {

    if(!toc_.exists()) {
        Log::info() << "TOC doesn't exist " << toc_.filePath() << std::endl;
        return false;
    }

    return true;
}

void TocDBReader::axis(const Key& key, const std::string& keyword, StringSet& s) const
{
    const std::vector<PathName>& indexesPaths = toc_.indexes( key );

    for( std::vector<PathName>::const_iterator itr = indexesPaths.begin(); itr != indexesPaths.end(); ++itr )
    {
        const Index& idx = getIndex(*itr);
        const eckit::StringSet& a = idx.axis().values(keyword);
        s.insert(a.begin(), a.end());
    }
}

void TocDBReader::close() {
}

eckit::DataHandle* TocDBReader::retrieve(const Key& key) const
{
    Log::info() << "Trying to retrieve key " << key << std::endl;

    Log::info() << "Scanning indexes " << current_ << std::endl;

    const Index* index = 0;

    Index::Field field;
    for( std::vector<PathName>::const_iterator itr = current_.begin(); itr != current_.end(); ++itr )
    {
        const Index& idx = getIndex(*itr);

        if( idx.get(key, field) )
        {
            index = &idx;
            break;
        }
    }

    if( ! index ) // not found
        return 0;
    else
        return field.path_.partHandle(field.offset_, field.length_);
}

Index* TocDBReader::openIndex(const PathName& path) const
{
    return Index::create( indexType_, path, Index::READ );
}

void TocDBReader::print(std::ostream &out) const
{
    out << "TocDBReader(" << toc_ << ")";
}

DBBuilder<TocDBReader> TocDBReader_builder("toc.reader");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
