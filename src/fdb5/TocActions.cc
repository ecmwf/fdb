/*
 * (C) Copyright 1996-2013 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <cstring>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include "eckit/io/FileHandle.h"
#include "eckit/exception/Exceptions.h"

#include "fdb5/Error.h"
#include "fdb5/TocActions.h"
#include "fdb5/TocSchema.h"

using namespace eckit;

namespace fdb5 {

//-----------------------------------------------------------------------------

TocInitialiser::TocInitialiser(const PathName& dir) : TocHandler()
{    
    dir_ = dir; // not initialized by TocHandler else it will check it exists

	if( !dir_.exists() )
	{
		dirPath().mkdir();
	}

	if( !dir_.isDir() )
		throw eckit::UnexpectedState( dir_ + " is not a directory" );

    /// @TODO copy the fdb schema into the directory

	if( !filePath().exists() )
	{
        int iomode = O_WRONLY | O_CREAT | O_EXCL;
        fd_ = ::open( filePath().asString().c_str(), iomode, (mode_t)0777 );
		read_   = false;

		if( fd_ >= 0 ) // successfully created
		{
			TocRecord r = makeRecordTocInit();
			append(r);
			close();
		}
        else {
            if( errno == EEXIST ) {
                Log::warning() << "TocInitialiser: " << filePath() << " already exists" << std::endl;
            }
            else {
                SYSCALL2(fd_, filePath());
            }
        }

	}
}

//-----------------------------------------------------------------------------

TocIndex::TocIndex(const PathName& dir, const PathName& idx, const TocRecord::MetaData& md) :
    TocHandler( dir ),
    index_(idx),
    tocMD_(md)
{
}

TocIndex::TocIndex(const TocSchema& schema, const Key& key) :
    TocHandler( schema.tocDirPath() ),
    index_( schema.generateIndexPath(key) ),
    tocMD_( schema.tocEntry(key) )
{
}

TocIndex::~TocIndex()
{
	openForAppend();
	TocRecord r = makeRecordIdxInsert(index_,tocMD_);
	append(r);
	close();
}
eckit::PathName TocIndex::index() const
{
    return index_;
}

//-----------------------------------------------------------------------------

TocReverseIndexes::TocReverseIndexes(const PathName& dir) : TocHandler(dir)
{
	openForRead();

	TocRecord r;

    while( readNext(r) ) {
		toc_.push_back(r);
    }

	close();
}

std::vector<PathName> TocReverseIndexes::indexes(const TocRecord::MetaData& md) const
{
	TocMap::const_iterator f = cacheIndexes_.find(md);

	if( f != cacheIndexes_.end() )
		return f->second;

	// not in cache

	std::vector< PathName > indexes;

	for( TocVec::const_iterator itr = toc_.begin(); itr != toc_.end(); ++itr )
	{
		const TocRecord& r = *itr;
		switch (r.head_.tag_)
		{
			case TOC_INIT: // ignore the Toc initialisation
			break;

			case TOC_INDEX:
			if( r.metadata() == md )
				indexes.push_back( r.path() );
			break;

			case TOC_CLEAR:
			if( r.metadata() == md )
				indexes.erase( std::remove( indexes.begin(), indexes.end(), r.path() ), indexes.end() );
			break;

			case TOC_WIPE:
				indexes.clear();
			break;

			default:
				throw SeriousBug("Unknown tag in TocRecord",Here());
			break;
		}
	}

	std::reverse(indexes.begin(), indexes.end()); // the entries of the last index takes precedence

	cacheIndexes_[ md ] = indexes; // cache before returning

	return indexes;
}

//-----------------------------------------------------------------------------

TocList::TocList(const PathName& dir) : TocHandler(dir)
{
	openForRead();

	TocRecord r;

	while( readNext(r) )
		toc_.push_back(r);

	close();
}

const std::vector<TocRecord>& TocList::list()
{
	return toc_;
}

void TocList::list(std::ostream& out) const
{
	for( TocVec::const_iterator itr = toc_.begin(); itr != toc_.end(); ++itr )
	{
		const TocRecord& r = *itr;
		switch (r.head_.tag_)
		{
			case TOC_INIT:
				out << "TOC_INIT " << r << std::endl;
			break;

			case TOC_INDEX:
				out << "TOC_INDEX " << r << std::endl;
			break;

			case TOC_CLEAR:
				out << "TOC_CLEAR " << r << std::endl;
			break;

			case TOC_WIPE:
				out << "TOC_WIPE " << r << std::endl;
			break;

			default:
				throw SeriousBug("Unknown tag in TocRecord",Here());
			break;
		}
	}
}

//-----------------------------------------------------------------------------

TocPrint::TocPrint(const PathName& dir) : TocHandler(dir)
{
}

void TocPrint::print(std::ostream& os)
{
	openForRead();

	TocRecord r;

	while( readNext(r) )
	{
		printRecord(r,os);
		os << std::endl;
	}

	close();
}

//-----------------------------------------------------------------------------

} // namespace fdb5
