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
#include "eckit/config/Resource.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/serialisation/HandleStream.h"

#include "fdb5/TocHandler.h"
#include "fdb5/Key.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocHandler::TocHandler(const eckit::PathName& dir) :
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

    eckit::Log::info() << "Opening for append TOC " << filePath() << std::endl;

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

    eckit::Log::info() << "Opening for read TOC " << filePath() << std::endl;

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

size_t TocHandler::readNext( TocRecord& r )
{
	ASSERT( isOpen() && read_ );

    int len;

    SYSCALL2( len = ::read(fd_, &r, sizeof(TocRecord)), filePath() );
    if(len == 0) {
        return len;
    }

    if(len != sizeof(TocRecord))
    {
        close();
        std::ostringstream msg;
        msg << "Failed to read complete TocRecord from " << filePath().asString();
        throw eckit::ReadError( msg.str() );
    }

    if( TocRecord::currentTagVersion() != r.version() ) {
        std::ostringstream oss;
        oss << "Record version mistach, expected " << int(TocRecord::currentTagVersion())
            << ", got " << int(r.version());
        throw eckit::SeriousBug(oss.str());
    }

    ASSERT( r.isComplete() );

    return len;
}

void TocHandler::close()
{
	if( isOpen() )
	{
        eckit::Log::info() << "Closing TOC " << filePath() << std::endl;

        if(!read_) {
            int ret = fsync(fd_);

            while (ret < 0 && errno == EINTR)
                ret = fsync(fd_);

            if (ret < 0) {
                eckit::Log::error() << "Cannot fsync(" << filePath() << ") " << fd_ <<  eckit::Log::syserr << std::endl;
            }

            // On Linux, you must also flush the directory
            static bool fileHandleSyncsParentDir = eckit::Resource<bool>("fileHandleSyncsParentDir", true);
            if( fileHandleSyncsParentDir )
                filePath().syncParentDirectory();
        }

		SYSCALL2( ::close(fd_), filePath() );
		fd_ = -1;
	}
}

void TocHandler::printRecord(const TocRecord& r, std::ostream& os)
{
	switch (r.head_.tag_)
	{
		case TOC_INIT:
			os << "Record TocInit [" << eckit::DateTime( r.head_.timestamp_.tv_sec ) << "]";
		break;

		case TOC_INDEX:
			os << "Record IdxFinal [" << eckit::DateTime( r.head_.timestamp_.tv_sec ) << "]";
		break;

		case TOC_CLEAR:
			os << "Record IdxCancel [" << eckit::DateTime( r.head_.timestamp_.tv_sec ) << "]";
		break;

		case TOC_WIPE:
			os << "Record TocWipe [" << eckit::DateTime( r.head_.timestamp_.tv_sec ) << "]";
		break;

		default:
			throw eckit::SeriousBug("Unknown tag in TocRecord",Here());
		break;
	}
}

TocRecord TocHandler::makeRecordTocInit(const Key& key) const
{
    TocRecord r( TOC_INIT );

    eckit::MemoryHandle handle(r.payload_.data(), r.payload_size);
    handle.openForWrite(0);
    eckit::AutoClose close(handle);
    eckit::HandleStream s(handle);

    s << key;

	return r;
}

TocRecord TocHandler::makeRecordIdxInsert(const eckit::PathName& path, const Key& key ) const
{
    TocRecord r( TOC_INDEX );

    eckit::MemoryHandle handle(r.payload_.data(), r.payload_size);
    handle.openForWrite(0);
    eckit::AutoClose close(handle);
    eckit::HandleStream s(handle);

    s << key;
    s <<  path.baseName();

	return r;
}

TocRecord TocHandler::makeRecordIdxRemove(const eckit::PathName& path) const
{
    TocRecord r( TOC_CLEAR );

    eckit::MemoryHandle handle(r.payload_.data(), r.payload_size);
    handle.openForWrite(0);
    eckit::AutoClose close(handle);
    eckit::HandleStream s(handle);

    s <<  path.baseName();

    return r;
}

TocRecord TocHandler::makeRecordTocWipe() const
{
    TocRecord r( TOC_WIPE );
    return r;
}

void TocHandler::print(std::ostream& out) const {
    out << "TocHandler[path=" << filePath() << "]";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
