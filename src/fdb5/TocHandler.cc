/*
 * (C) Copyright 1996-2013 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "eckit/types/DateTime.h"

#include "fdb5/TocHandler.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocHandler::TocHandler(const PathName& dir) :
    dir_(dir),
    filePath_(dir_ / "toc"),
    fd_(-1),
    read_(false)
{
}

TocHandler::~TocHandler()
{
    close();
}

bool TocHandler::exists() const {
    return filePath_.exists();
}

void TocHandler::openForAppend()
{
	ASSERT( !isOpen() );

    Log::info() << "Opening for append TOC " << filePath() << std::endl;

    int iomode = O_WRONLY | O_APPEND;
//#ifdef __linux__
//	iomode |= O_NOATIME;
//#endif
    SYSCALL2((fd_ = ::open( filePath().asString().c_str(), iomode, (mode_t)0777 )), filePath());
	read_   = false;
}

void TocHandler::openForRead()
{
	ASSERT( !isOpen() );

    Log::info() << "Opening for read TOC " << filePath() << std::endl;

    int iomode = O_RDONLY;
//#ifdef __linux__
//	iomode |= O_NOATIME;
//#endif
    SYSCALL2((fd_ = ::open( filePath().asString().c_str(), iomode )), filePath() );
	read_   = true;
}

void TocHandler::append( const TocRecord& r )
{
	ASSERT( isOpen() && !read_ );

	try
	{
        size_t len;
        SYSCALL2( len = ::write(fd_, &r, sizeof(TocRecord)), filePath() );
        ASSERT( len == sizeof(TocRecord) );
	}
	catch(...)
	{
		close();
		throw;
	}
}

Length TocHandler::readNext( TocRecord& r )
{
	ASSERT( isOpen() && read_ );

    Length len;

    SYSCALL2( len = ::read(fd_, &r, sizeof(TocRecord)), filePath() );

    ASSERT( r.isComplete() );

    if( len != 0 && len != sizeof(TocRecord) )
	{
		close();
		std::ostringstream msg;
		msg << "Failed to read complete TocRecord from " << filePath().asString();
		throw ReadError( msg.str() );
	}

    return len;
}

void TocHandler::close()
{
	if( isOpen() )
	{
        Log::info() << "Closing TOC " << filePath() << std::endl;

		SYSCALL2( ::close(fd_), filePath() );
		fd_ = -1;
	}
}

void TocHandler::printRecord(const TocRecord& r, std::ostream& os)
{
	switch (r.head_.tag_)
	{
		case TOC_INIT:
			os << "Record TocInit [" << DateTime( r.head_.timestamp_.tv_sec ) << "]";
		break;

		case TOC_INDEX:
			os << "Record IdxFinal [" << DateTime( r.head_.timestamp_.tv_sec ) << "]";
		break;

		case TOC_CLEAR:
			os << "Record IdxCancel [" << DateTime( r.head_.timestamp_.tv_sec ) << "]";
		break;

		case TOC_WIPE:
			os << "Record TocWipe [" << DateTime( r.head_.timestamp_.tv_sec ) << "]";
		break;

		default:
			throw SeriousBug("Unknown tag in TocRecord",Here());
		break;
	}
}

TocRecord TocHandler::makeRecordTocInit() const
{
	TocRecord r( TOC_INIT, (unsigned char)(1) );
	return r;
}

TocRecord TocHandler::makeRecordIdxInsert( const eckit::PathName& path, const TocRecord::MetaData& md ) const
{
	TocRecord r( TOC_INDEX, (unsigned char)(1) );
	r.metadata_ = md;
	r.payload_  = path.asString();
	return r;
}

TocRecord TocHandler::makeRecordIdxRemove() const
{
	TocRecord r( TOC_CLEAR, (unsigned char)(1) );
	r.init();
	return r;
}

TocRecord TocHandler::makeRecordTocWipe() const
{
	TocRecord r( TOC_WIPE, (unsigned char)(1) );
    return r;
}

void TocHandler::print(std::ostream& out) const {
    out << "TocHandler(" << filePath() << ")";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
