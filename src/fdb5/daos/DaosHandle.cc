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
#include "fdb5/daos/DaosSession.h"

using eckit::Length;
using eckit::Offset;

namespace fdb5 {

DaosHandle::DaosHandle(fdb5::DaosObject&& obj) : 
    obj_(std::unique_ptr<fdb5::DaosObject>(new DaosObject(std::move(obj)))),
    open_(false), offset_(0) {}

DaosHandle::DaosHandle(fdb5::DaosSession& session, const fdb5::DaosName& name) : 
    obj_(std::unique_ptr<fdb5::DaosObject>(new DaosObject(session, name))),
    open_(false), offset_(0) {}

DaosHandle::~DaosHandle() {

    // TODO: logging could throw?
    if (open_) eckit::Log::error() << "DaosHandle not closed before destruction." << std::endl;

}

void DaosHandle::openForWrite(const Length& len) {

    if (open_) NOTIMP;

    // no need to create obj_ in case it doesn't exist, it always exists due to imperative approach

    // if the handle has been created from a DaosName/URI which contains a fully specified OID which
    // does not exist, it won't be created

    obj_->open();

    // TODO: should wipe object content?

    open_ = true;

    offset_ = eckit::Offset(0);

}

void DaosHandle::openForAppend(const Length& len) {

    if (open_) NOTIMP;

    obj_->open();

    open_ = true;

    // TODO: should offset be set to size() or be left to its current value?
    offset_ = eckit::Offset(obj_->size());

}

Length DaosHandle::openForRead() {

    if (open_) NOTIMP;

    obj_->open();

    open_ = true;

    offset_ = eckit::Offset(0);

    return Length(obj_->size());

}

long DaosHandle::write(const void* buf, long len) {

    ASSERT(open_);

    long written = obj_->write(buf, len, offset_);

    offset_ += written;

    return written;

}

long DaosHandle::read(void* buf, long len) {

    ASSERT(open_);

    long read = obj_->read(buf, len, offset_);

    offset_ += read;

    return read;

}

void DaosHandle::close() {

    obj_->close();

    open_ = false;

    // TODO: should offset be set to 0?

}

void DaosHandle::flush() {

    // empty implmenetation

}

Length DaosHandle::size() {

    // TODO: is this assert needed? As long as obj is open, should be ok
    ASSERT(open_);
    return Length(obj_->size());

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
    
    return obj_->name();

}

void DaosHandle::print(std::ostream& s) const {
    s << "DaosHandle[notimp]";
}

}  // namespace fdb5