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
    toc_(schema_.tocDirPath())
{
    Log::info() << "TocDBReader for TOC [" << schema_.tocDirPath() << "]" << std::endl;
}

TocDBReader::~TocDBReader()
{
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
    const std::vector<PathName>& indexesPaths = toc_.indexes( schema_.tocEntry(key) );

    for( std::vector<PathName>::const_iterator itr = indexesPaths.begin(); itr != indexesPaths.end(); ++itr )
    {
        const Index& idx = getIndex(*itr);
        const eckit::StringSet& a = idx.axis().values(keyword);
        s.insert(a.begin(), a.end());
    }
}

void TocDBReader::close() {
}

eckit::DataHandle* TocDBReader::retrieve(const MarsTask& task, const Key& key) const
{
    if(!match(key)) return 0;

    Log::info() << "Trying to retrieve key " << key << std::endl;

    const std::vector<PathName>& indexesPaths = toc_.indexes( schema_.tocEntry(key) );

    Log::info() << "Scanning indexes " << indexesPaths << std::endl;

    const Index* index = 0;

    Key k( schema_.dataIdx(key) );

    Index::Field field;
    for( std::vector<PathName>::const_iterator itr = indexesPaths.begin(); itr != indexesPaths.end(); ++itr )
    {
        const Index& idx = getIndex(*itr);

        if( idx.get(k, field) )
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
    return Index::create( schema_.indexType(), path, Index::READ );
}

void TocDBReader::print(std::ostream &out) const
{
    out << "TocDBReader(" << toc_ << ")";
}

DBBuilder<TocDBReader> TocDBReader_builder("toc.reader");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
