/*
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-3.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */


/// @author Simon Smart
/// @date   Dec 2016

#include <cstring>

#include "eckit/exception/Exceptions.h"

#include "fdb5/pmem/MemoryBufferStream.h"

using namespace eckit;

namespace fdb5 {
namespace pmem {

// -------------------------------------------------------------------------------------------------


MemoryBufferStream::MemoryBufferStream() :
    size_(4096),
    position_(0),
    buffer_(size_) {}


MemoryBufferStream::~MemoryBufferStream() {}


long MemoryBufferStream::read(void* buffer, long length) {

    // Intentionally not implemented. This is a write buffer.
    NOTIMP;

    (void) buffer;
    (void) length;
}


long MemoryBufferStream::write(const void* buffer, long length) {

    // If the buffer is full, then reallocate it to be bigger.

    Length remaining = size_ - position_;
    if (remaining < Length(length)) {
        MemoryBuffer tmp(size_ * 2);
        ::memcpy(tmp, buffer_, size_);
        buffer_.swap(tmp);
        size_ = size_ * 2;
    }

    ::memcpy(buffer_ + position_, buffer, length);
    position_ += length;

    return length;
}


void MemoryBufferStream::rewind()
{
    position_ = 0;
}


std::string MemoryBufferStream::name() const {
    return "MemoryBufferStream";
}


size_t MemoryBufferStream::position() const {
    return position_;
}


const MemoryBuffer& MemoryBufferStream::buffer() const {
    return buffer_;
}

// -------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5
