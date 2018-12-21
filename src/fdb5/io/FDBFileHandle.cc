/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <stdio.h>
#include <unistd.h>

#include "eckit/eckit.h"
#include "eckit/config/Resource.h"
#include "eckit/log/Log.h"

#include "fdb5/LibFdb.h"
#include "fdb5/io/FDBFileHandle.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

void FDBFileHandle::print(std::ostream &s) const {
    s << "FDBFileHandle[file=" << path_ << ']';
}

FDBFileHandle::FDBFileHandle(const std::string &name, size_t buffer):
    path_(name),
    file_(0),
    buffer_(buffer) {
}

FDBFileHandle::~FDBFileHandle() {
}

Length FDBFileHandle::openForRead() {
    NOTIMP;
}

void FDBFileHandle::openForWrite(const Length&) {
    NOTIMP;
}

void FDBFileHandle::openForAppend(const Length&) {
    file_ = ::fopen(path_.c_str(), "a");
    if (!file_) {
        throw eckit::CantOpenFile(path_);
    }

    SYSCALL(::setvbuf(file_, buffer_, _IOFBF, buffer_.size()));
}

long FDBFileHandle::read(void*, long) {
    NOTIMP;
}

long FDBFileHandle::write(const void *buffer, long length) {
    ASSERT( buffer );

    long written = ::fwrite(buffer, 1, length, file_);

    if (written != length) {
        throw eckit::WriteError(path_);
    }

    return written;
}

void FDBFileHandle::flush() {

    static bool fdbDataSyncOnFlush = eckit::LibResource<bool,LibFdb>("$FDB_DATA_SYNC_ON_FLUSH;fdbDataSyncOnFlush", true);

    if (file_) {

        if(::fflush(file_)) throw WriteError(std::string("FDBFileHandle::~FDBFileHandle(fflush(") + path_ + "))", Here());

        if(fdbDataSyncOnFlush) {

            int ret = ::fdatasync(::fileno(file_));

            while (ret < 0 && errno == EINTR) {
                ret = ::fdatasync(::fileno(file_));
            }
            if (ret < 0) {
                Log::error() << "Cannot fdatasync(" << path_ << ") " << ::fileno(file_) <<  Log::syserr << std::endl;
            }
        }
    }
}

void FDBFileHandle::close() {
    if (file_) {
        if (::fclose(file_)) {
            file_ = 0;
            throw WriteError(std::string("fclose ") + name());
        }
        file_ = 0;
    }
}

Offset FDBFileHandle::position() {
    ASSERT(file_);
    return ::ftello(file_);
}

std::string FDBFileHandle::title() const {
    return PathName::shorten(path_);
}


//----------------------------------------------------------------------------------------------------------------------

} // namespace eckit
