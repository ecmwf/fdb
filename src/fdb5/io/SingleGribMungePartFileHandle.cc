/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "fdb5/io/SingleGribMungePartFileHandle.h"

#include <algorithm>

#include "eckit/log/Log.h"

#include <cstdio>
#include "eccodes.h"

using namespace eckit;

namespace fdb5 {

//--------------------------------------------------------------------------------------------------

::eckit::ClassSpec SingleGribMungePartFileHandle::classSpec_ = {
    &DataHandle::classSpec(),
    "SingleGribMungePartFileHandle",
};
::eckit::Reanimator<SingleGribMungePartFileHandle> SingleGribMungePartFileHandle::reanimator_;

void SingleGribMungePartFileHandle::print(std::ostream& s) const {
    if (format(s) == Log::compactFormat)
        s << "SingleGribMungePartFileHandle";
    else
        s << "SingleGribMungePartFileHandle[path=" << name_ << ",offset=" << offset_ << ",length=" << length_ << ']';
}

SingleGribMungePartFileHandle::SingleGribMungePartFileHandle(const PathName& name, const Offset& offset,
                                                             const Length& length, const Key& substitute) :
    name_(name), file_(nullptr), pos_(0), offset_(offset), length_(length), substitute_(substitute) {}


DataHandle* SingleGribMungePartFileHandle::clone() const {
    return new SingleGribMungePartFileHandle(name_, offset_, length_, substitute_);
}


bool SingleGribMungePartFileHandle::compress(bool) {
    return false;
}

SingleGribMungePartFileHandle::~SingleGribMungePartFileHandle() {
    if (file_) {
        Log::warning() << "Closing SingleGribMungePartFileHandle " << name_ << std::endl;
        ::fclose(file_);
        file_ = nullptr;
    }
}

Length SingleGribMungePartFileHandle::openForRead() {

    pos_  = 0;
    file_ = ::fopen(name_.localPath(), "r");

    if (!file_)
        throw CantOpenFile(name_, errno == ENOENT);

    if (buffer_) {
        buffer_.reset();
    }

    return estimate();
}

long SingleGribMungePartFileHandle::read(void* buffer, long length) {

    ASSERT(file_);

    // Read the data into a buffer, and convert the message to a new one

    if (!buffer_) {

        Buffer readBuffer(length_);

        // Read the data in from the relevant file

        off_t off = offset_;
        if (::fseeko(file_, off, SEEK_SET) != 0) {
            std::ostringstream ss;
            ss << name_ << ": cannot seek to " << off << " (file=" << fileno(file_) << ")";
            throw ReadError(ss.str());
        }

        off_t pos;
        SYSCALL(pos = ::ftello(file_));
        ASSERT(pos == off);
        ASSERT(::fread(readBuffer, 1, length_, file_) == static_cast<size_t>(length_));

        // Do the GRIB manipulation

        codes_handle* handle = codes_handle_new_from_message(0, readBuffer, length_);
        ASSERT(handle != 0);

        for (const auto& kv : substitute_) {
            size_t valueSize = kv.second.length();
            CODES_CHECK(codes_set_string(handle, kv.first.c_str(), kv.second.c_str(), &valueSize), 0);
        }

        size_t messageSize        = 0;
        const void* messageBuffer = 0;
        CODES_CHECK(codes_get_message(handle, &messageBuffer, &messageSize), 0);

        buffer_.reset(new Buffer(messageSize));
        pos_ = 0;
        ::memcpy(*buffer_, messageBuffer, messageSize);

        codes_handle_delete(handle);
    }

    long readLength = 0;

    if (pos_ < length_) {
        long readLength = std::min(static_cast<long>(length_ - pos_), length);
        ::memcpy(buffer, &(*buffer_)[pos_], readLength);
        pos_ += readLength;
        return readLength;
    }

    return readLength;
}

void SingleGribMungePartFileHandle::close() {
    if (file_) {
        ::fclose(file_);
        file_ = 0;
    }
    else {
        Log::warning() << "Closing SingleGribMungePartFileHandle " << name_ << ", file is not opened" << std::endl;
    }
    buffer_.reset();
}

bool SingleGribMungePartFileHandle::merge(DataHandle*) {
    return false;
}

Length SingleGribMungePartFileHandle::size() {
    return length_;
}

Length SingleGribMungePartFileHandle::estimate() {
    return length_;
}

std::string SingleGribMungePartFileHandle::title() const {
    return PathName::shorten(name_);
}

//--------------------------------------------------------------------------------------------------

}  // namespace fdb5
