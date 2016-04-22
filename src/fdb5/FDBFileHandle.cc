/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <sys/stat.h>
#include <dirent.h>

#include "eckit/eckit.h"

#include "eckit/config/Resource.h"
#include "eckit/io/MarsFSHandle.h"
#include "eckit/filesystem/marsfs/MarsFSPath.h"
#include "eckit/io/cluster/NodeInfo.h"
#include "eckit/log/Bytes.h"
#include "eckit/log/Log.h"
#include "eckit/os/Stat.h"

#include "fdb5/FDBFileHandle.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

ClassSpec FDBFileHandle::classSpec_ = {&DataHandle::classSpec(),"FDBFileHandle",};
Reanimator<FDBFileHandle> FDBFileHandle::reanimator_;

void FDBFileHandle::print(std::ostream& s) const
{
    s << "FDBFileHandle[file=" << name_ << ']';
}

void FDBFileHandle::encode(Stream& s) const
{
    DataHandle::encode(s);
    s << name_;
    s << overwrite_;
}

FDBFileHandle::FDBFileHandle(Stream& s):
        DataHandle(s),
        file_(0),
        read_(false),
        overwrite_(false)
{
    s >> name_;
    s >> overwrite_;
}

FDBFileHandle::FDBFileHandle(const std::string& name,bool overwrite):
    name_(name),
    file_(0),
    read_(false),
    overwrite_(overwrite)
{
}

FDBFileHandle::~FDBFileHandle()
{
}

void FDBFileHandle::open(const char* mode)
{
    file_ = ::fopen(name_.c_str(),mode);
    if (file_ == 0)
        throw CantOpenFile(name_);

    // Don't buffer writes, so we know when the filesystems
    // are full at fwrite time, and not at fclose time.
    // There should not be any performances issues as
    // this class is used with large buffers anyway

    if (!(::strcmp(mode,"r") == 0))
        setbuf(file_,0);
	else
	{
        static long bufSize  = Resource<long>("FileHandleIOBufferSize;$FILEHANDLE_IO_BUFFERSIZE;-FileHandleIOBufferSize",0);
		long size = bufSize;
		if(size)
		{
            Log::debug() << "FDBFileHandle using " << Bytes(size) << std::endl;
			buffer_.reset(new Buffer(size));
			Buffer& b = *(buffer_.get());
			::setvbuf(file_,b,_IOFBF,size);
		}
	}

    //Log::info() << "FDBFileHandle::open " << name_ << " " << mode << " " << fileno(file_) << std::endl;

}

Length FDBFileHandle::openForRead()
{
    read_ = true;
    open("r");
    return estimate();
}

void FDBFileHandle::openForWrite(const Length& length)
{
    read_ = false;
    PathName path(name_);
    // This is for preallocated files
    if (overwrite_ && path.exists())
    {
        ASSERT(length == path.size());
        open("r+");
    }
    else
        open("w");
}

void FDBFileHandle::openForAppend(const Length&)
{
    open("a");
}

long FDBFileHandle::read(void* buffer,long length)
{
    return ::fread(buffer,1,length,file_);
}

long FDBFileHandle::write(const void* buffer,long length)
{
	ASSERT( buffer );

	long written = ::fwrite(buffer,1,length,file_);

    /// @note In contrast to FileHandle, we do not wait on ENOSPC

    return written;
}

void FDBFileHandle::sync()
{
    int ret = fsync(fileno(file_));

    while (ret < 0 && errno == EINTR)
        ret = fsync(fileno(file_));
    if (ret < 0) {
        Log::error() << "Cannot fsync(" << name_ << ") " <<fileno(file_) <<  Log::syserr << std::endl;
    }

    // On Linux, you must also flush the directory
    static bool fileHandleSyncsParentDir = eckit::Resource<bool>("fileHandleSyncsParentDir", true);
    if( fileHandleSyncsParentDir )
        PathName(name_).syncParentDirectory();
}

void FDBFileHandle::flush()
{
    if( file_ == 0 ) return;

    if (file_ && !read_) {
        if (::fflush(file_))
                throw WriteError(std::string("FDBFileHandle::~FDBFileHandle(fflush(") + name_ + "))");
    }
}

void FDBFileHandle::close()
{
    if( file_ == 0 ) return;

    if (file_)
    {
        // Because AIX has large system buffers,
        // the close may be successful without the
        // data being physicaly on disk. If there is
        // a power failure, we lose some data. So we
        // need to fsync

        flush();

        sync();

        if (::fclose(file_) != 0)
        {
            throw WriteError(std::string("fclose ") + name());
        }
    }
    else
    {
        Log::warning() << "Closing FDBFileHandle " << name_ << ", file is not opened" << std::endl;
    }
    buffer_.reset(0);
}

void FDBFileHandle::rewind()
{
    ::rewind(file_);
}

Length FDBFileHandle::estimate()
{
    Stat::Struct info;
    SYSCALL(Stat::stat(name_.c_str(),&info));
    return info.st_size;
}

bool FDBFileHandle::isEmpty() const
{
    Stat::Struct info;
    if( Stat::stat(name_.c_str(),&info) == -1 )
        return false; // File does not exists
    return info.st_size == 0;
}

// Try to be clever ....

Length FDBFileHandle::saveInto(DataHandle& other,TransferWatcher& w)
{
    static bool fileHandleSaveIntoOptimisationUsingHardLinks = eckit::Resource<bool>("fileHandleSaveIntoOptimisationUsingHardLinks", false);
    if(!fileHandleSaveIntoOptimisationUsingHardLinks)
        return DataHandle::saveInto(other,w);

    // Poor man's RTTI,
    // Does not support inheritance

    if ( !sameClass(other) )
        return DataHandle::saveInto(other,w);

    // We should be safe to cast now....

    FDBFileHandle* handle = dynamic_cast<FDBFileHandle*>(&other);

    if (::link(name_.c_str(),handle->name_.c_str()) == 0)
        Log::debug() << "Saved ourselves a file to file copy!!!" << std::endl;
    else {
        Log::debug() << "Failed to link " <<  name_
        << " to " << handle->name_ << Log::syserr << std::endl;
        Log::debug() << "Using defaut method" << std::endl;
        return DataHandle::saveInto(other,w);
    }

    return estimate();
}

Offset FDBFileHandle::position()
{
    ASSERT(file_);
    return ::ftello(file_);
}

void FDBFileHandle::advance(const Length& len)
{
    off_t l = len;
    if (::fseeko(file_,l,SEEK_CUR) < 0)
        throw ReadError(name_);
}

void FDBFileHandle::restartReadFrom(const Offset& from)
{
    ASSERT(read_);
    Log::warning() << *this << " restart read from " << from << std::endl;
    off_t l = from;
    if (::fseeko(file_,l,SEEK_SET) < 0)
        throw ReadError(name_);

    ASSERT(::ftello(file_) == l);
}

void FDBFileHandle::restartWriteFrom(const Offset& from)
{
    ASSERT(!read_);
    Log::warning() << *this << " restart write from " << from << std::endl;
    off_t l = from;
    if (::fseeko(file_,l,SEEK_SET) < 0)
        throw ReadError(name_);

    ASSERT(::ftello(file_) == l);
}

Offset FDBFileHandle::seek(const Offset& from)
{
    off_t l = from;
    if (::fseeko(file_,l,SEEK_SET) < 0)
        throw ReadError(name_);
    off_t w = ::ftello(file_);
    ASSERT(w == l);
    return w;
}

void FDBFileHandle::skip(const Length &n)
{
    off_t l = n;
    if (::fseeko(file_,l,SEEK_CUR) < 0)
        throw ReadError(name_);
}

void FDBFileHandle::toRemote(Stream& s) const
{
    MarsFSPath p(PathName(name_).clusterName());
    MarsFSHandle remote(p);
    s << remote;
}

void FDBFileHandle::cost(std::map<std::string,Length>& c, bool read) const
{
    if (read)
    {
        c[NodeInfo::thisNode().node()] += const_cast<FDBFileHandle*>(this)->estimate();
    }
    else
    {
        // Just mark the node as being a candidate
        c[NodeInfo::thisNode().node()] += 0;
    }
}

std::string FDBFileHandle::title() const
{
    return PathName::shorten(name_);
}


DataHandle* FDBFileHandle::clone() const
{
    return new FDBFileHandle(name_, overwrite_);
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace eckit
