/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

// #include <daos.h>

#include <memory>

// #include "eckit/config/Resource.h"
// #include "eckit/utils/Tokenizer.h"
#include "eckit/exception/Exceptions.h"

#include "fdb5/daos/DaosHandle.h"
#include "fdb5/daos/DaosPool.h"
#include "fdb5/daos/DaosContainer.h"
#include "fdb5/daos/DaosObject.h"
// #include "fdb5/daos/DaosCluster.h"

using eckit::Length;
using eckit::Offset;

namespace fdb5 {

// TODO: do we really want to allow this? handle now manages an also user-managed object. Should we std::move it?
DaosHandle::DaosHandle(fdb5::DaosObject& obj) : obj_(obj), open_(false), offset_(0) {}

DaosHandle::DaosHandle(const fdb5::DaosName& name) : 
    managed_obj_(std::unique_ptr<fdb5::DaosObject>(new DaosObject(name))), 
    obj_(*(managed_obj_.get())), 
    open_(false), 
    offset_(0) {}

DaosHandle::~DaosHandle() {

    // TODO: do we really want to close? in cases where an object is managed by the user and multiple data handles are built from it, the handles may repeatedly open and close a same object
    if (open_) close();

}

void DaosHandle::openForWrite(const Length& len) {

    if (open_) NOTIMP;

    offset_ = eckit::Offset(0);

    // TODO: should wipe object content?

    obj_.create();

    open_ = true;

}

void DaosHandle::openForAppend(const Length& len) {

    if (open_) NOTIMP;

    // TODO: should the open crash if object does not exist?

    obj_.open();

    open_ = true;

    // TODO: should offset be set to size() or be left to its current value?
    offset_ = eckit::Offset(obj_.size());

}

Length DaosHandle::openForRead() {

    if (open_) NOTIMP;

    offset_ = eckit::Offset(0);

    obj_.open();

    open_ = true;

    return Length(obj_.size());

}

long DaosHandle::write(const void* buf, long len) {

    ASSERT(open_);

    long written = obj_.write(buf, len, offset_);

    offset_ += written;

    return written;

}

long DaosHandle::read(void* buf, long len) {

    ASSERT(open_);

    long read = obj_.read(buf, len, offset_);

    offset_ += read;

    return read;

}

void DaosHandle::close() {

    obj_.close();

    open_ = false;

    // TODO: should offset be set to 0?

}

void DaosHandle::flush() {

    // empty implmenetation

}

Length DaosHandle::size() {

    // TODO: is this assert needed? As long as obj is open, should be ok
    ASSERT(open_);
    return Length(obj_.size());

}

Length DaosHandle::estimate() {

    return size();

}

Offset DaosHandle::position() {

    // TODO: should position() crash if unopened?
    return offset_;

}

Offset DaosHandle::seek(const Offset& offset) {

    offset_ = offset;
    // TODO: assert offset <= size() ?
    return offset_;

}

bool DaosHandle::canSeek() const {

    return true;

}

void DaosHandle::skip(const Length& len) {

    offset_ = Offset(len);

}

std::string DaosHandle::title() const {
    
    return obj_.name().asString();

}

void DaosHandle::print(std::ostream& s) const {
    s << "DaosHandle[notimp]";
}

}  // namespace fdb5