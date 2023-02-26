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

#include "eckit/exception/Exceptions.h"

#include "fdb5/daos/DaosArrayHandle.h"
#include "fdb5/daos/DaosPool.h"
#include "fdb5/daos/DaosContainer.h"
#include "fdb5/daos/DaosObject.h"
#include "fdb5/daos/DaosSession.h"
#include "fdb5/daos/DaosException.h"

using eckit::Length;
using eckit::Offset;

namespace fdb5 {

DaosArrayHandle::DaosArrayHandle(const fdb5::DaosArrayName& name) : name_(name), open_(false), offset_(0) {}

DaosArrayHandle::~DaosArrayHandle() {

    if (open_) eckit::Log::error() << "DaosArrayHandle not closed before destruction." << std::endl;

}

void DaosArrayHandle::print(std::ostream& s) const {
    s << "DaosArrayHandle[notimp]";
}

void DaosArrayHandle::openForWrite(const Length& len) {

    if (open_) NOTIMP;

    session_.reset(new fdb5::DaosSession());

    fdb5::DaosPool& p = session_->getPool(name_.poolName());
    fdb5::DaosContainer& c = p.ensureContainer(name_.contName());

    /// @todo: optionally remove this, as name_.OID() and OID generation are
    ///    triggered as part of DaosArray constructors.
    name_.generateOID();
    
    /// @todo: find a nicer way to check existence of an array?
    try {
        arr_.reset(new fdb5::DaosArray(*(session_.get()), name_));
    } catch (fdb5::DaosEntityNotFoundException& e) {
        arr_.reset(new fdb5::DaosArray( c.createArray(name_.OID()) ));
    }

    arr_->open();

    /// @todo: should wipe object content?

    open_ = true;

    offset_ = eckit::Offset(0);

}

void DaosArrayHandle::openForAppend(const Length& len) {

    openForWrite(len);

    /// @todo: should offset be set to size() or be left to its current value?
    offset_ = eckit::Offset(arr_->size());

}

Length DaosArrayHandle::openForRead() {

    if (open_) NOTIMP;

    session_.reset(new fdb5::DaosSession());

    name_.generateOID();

    arr_.reset(new fdb5::DaosArray(*(session_.get()), name_));

    arr_->open();

    open_ = true;

    offset_ = eckit::Offset(0);

    return Length(arr_->size());

}

long DaosArrayHandle::write(const void* buf, long len) {

    ASSERT(open_);

    long written = arr_->write(buf, len, offset_);

    offset_ += written;

    return written;

}

long DaosArrayHandle::read(void* buf, long len) {

    ASSERT(open_);

    long read = arr_->read(buf, len, offset_);

    offset_ += read;

    return read;

}

void DaosArrayHandle::close() {

    if (!open_) return;

    arr_->close();

    open_ = false;

    /// @todo: should offset be set to 0?

}

void DaosArrayHandle::flush() {

    /// @todo: should flush require closing?

    /// empty implmenetation

}

Length DaosArrayHandle::size() {

    return Length(name_.size());

}

Length DaosArrayHandle::estimate() {

    return size();

}

Offset DaosArrayHandle::position() {

    /// @todo: should position() crash if unopened?
    return offset_;

}

Offset DaosArrayHandle::seek(const Offset& offset) {

    offset_ = offset;
    /// @todo: assert offset <= size() ?
    return offset_;

}

bool DaosArrayHandle::canSeek() const {

    return true;

}

void DaosArrayHandle::skip(const Length& len) {

    offset_ = Offset(len);

}

std::string DaosArrayHandle::title() const {
    
    return name_.asString();

}

}  // namespace fdb5