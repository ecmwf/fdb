/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/FDBFileHandle.h"

/*
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
*/


using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

void FDBFileHandle::print(std::ostream& s) const
{
    s << "FDBFileHandle[file=" << name_ << ']';
}

FDBFileHandle::FDBFileHandle(const std::string& name,size_t buffer):
    name_(name),
    file_(0),
    buffer_(buffer)
{
}

FDBFileHandle::~FDBFileHandle()
{
}

/*
            Log::debug() << "FDBFileHandle using " << Bytes(size) << std::endl;
			buffer_.reset(new Buffer(size));
            */
Length FDBFileHandle::openForRead()
{
    NOTIMP;
}

void FDBFileHandle::openForWrite(const Length& length)
{
    NOTIMP;
}

void FDBFileHandle::openForAppend(const Length&)
{
    //open("a");
    file_ = fopen(name_.c_str(), "a");
    if(!file_) { 
        throw eckit::CantOpenFile(name_);
    }

    SYSCALL(::setvbuf(file_, buffer_, _IOFBF, buffer_.size()));
}

long FDBFileHandle::read(void* buffer,long length)
{
    NOTIMP;
}

long FDBFileHandle::write(const void* buffer,long length)
{
	ASSERT( buffer );

	long written = ::fwrite(buffer,1,length,file_);

    if(written != length) {
        throw eckit::WriteError(name_);
    }

    return written;
}

void FDBFileHandle::flush()
{
    if(file_) {
        if (::fflush(file_))
            throw WriteError(std::string("FDBFileHandle::~FDBFileHandle(fflush(") + name_ + "))");
    }
}

void FDBFileHandle::close()
{
    if(file_) 
    {
        if(::fclose(file_))
        {
            throw WriteError(std::string("fclose ") + name());
        }
    }
}

Offset FDBFileHandle::position()
{
    ASSERT(file_);
    return ::ftello(file_);
}

std::string FDBFileHandle::title() const
{
    return PathName::shorten(name_);
}


//----------------------------------------------------------------------------------------------------------------------

} // namespace eckit
