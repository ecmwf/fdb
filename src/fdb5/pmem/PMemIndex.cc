/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/io/Buffer.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/serialisation/Stream.h"

#include "fdb5/pmem/PMemIndex.h"
#include "fdb5/pmem/PBranchingNode.h"
#include "fdb5/pmem/PMemFieldLocation.h"

using namespace eckit;


namespace fdb5 {
namespace pmem {

//-----------------------------------------------------------------------------

// A little helper class that might end up somewhere else

class MemoryBufferStream : public Stream {

public: // methods

    MemoryBufferStream();
    ~MemoryBufferStream();

    virtual long read(void*,long);
    virtual long write(const void*,long);
    virtual void rewind();
    virtual std::string name() const;

    size_t position() const;
    const Buffer& buffer() const;

private: // members

    Length size_;
    Offset position_;

    Buffer buffer_;
};


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
        Buffer tmp(size_ * 2);
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


const Buffer& MemoryBufferStream::buffer() const {
    return buffer_;
}


//-----------------------------------------------------------------------------


PMemIndex::PMemIndex(const Key &key, PBranchingNode& node, const std::string& type) :
    Index(key, type),
    location_(node) {

    if (!location_.node().axis_.null()) {

        Log::error() << "** Loading axes from buffer" << std::endl;
        const ::pmem::PersistentBuffer& buf(*location_.node().axis_);
        MemoryStream s(buf.data(), buf.size());
        axes_.decode(s);
        Log::error() << "axes_ " << axes_ << std::endl;
    }
}


PMemIndex::~PMemIndex() {}


void PMemIndex::visitLocation(IndexLocationVisitor& visitor) const {
    visitor(location_);
}


bool PMemIndex::get(const Key &key, Field &field) const {
    NOTIMP;
}


void PMemIndex::open() {
    // Intentionally left blank. Indices neither opened nor closed (part of open DB).
}

void PMemIndex::reopen() {
    // Intentionally left blank. Indices neither opened nor closed (part of open DB).
}

void PMemIndex::close() {
    // Intentionally left blank. Indices neither opened nor closed (part of open DB).
}

void PMemIndex::add(const Key &key, const Field &field) {

    struct Inserter : FieldLocationVisitor {
        Inserter(PBranchingNode& indexNode, const Key& key) :
            indexNode_(indexNode),
            key_(key) {}

        virtual void operator() (const PMemFieldLocation& location) {
            indexNode_.insertDataNode(key_, location.node());
        }

    private:
        PBranchingNode& indexNode_;
        const Key& key_;
    };

    Inserter inserter(location_.node(), key);
    field.location().visit(inserter);

    // Keep track of the axes()

    if (axes().dirty()) {

        MemoryBufferStream s;
        axes().encode(s);

        const void* data = s.buffer();
        if (location_.node().axis_.null())
            location_.node().axis_.allocate(data, s.position());
        else
            location_.node().axis_.replace(data, s.position());

        axes_.clean();
    }
}

void PMemIndex::flush() {
    // Intentionally left blank. Flush not used in PMem case.
}

void PMemIndex::encode(Stream& s) const {
    NOTIMP;
}

void PMemIndex::entries(EntryVisitor &visitor) const {
    NOTIMP;
}

void PMemIndex::print(std::ostream &out) const {
    out << "PMemIndex[]";
}


std::string PMemIndex::defaulType() {
    return "PMemIndex";
}

void PMemIndex::dump(std::ostream& out, const char* indent, bool simple) const {
    NOTIMP;
}


//-----------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5
