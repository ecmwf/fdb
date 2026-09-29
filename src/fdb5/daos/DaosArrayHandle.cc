/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <memory>

#include "eckit/exception/Exceptions.h"

#include "fdb5/daos/DaosArrayHandle.h"
#include "fdb5/daos/DaosContainer.h"
#include "fdb5/daos/DaosException.h"
#include "fdb5/daos/DaosObject.h"
#include "fdb5/daos/DaosPool.h"
#include "fdb5/daos/DaosSession.h"

using eckit::Length;
using eckit::Offset;

namespace fdb5 {

DaosArrayHandle::DaosArrayHandle(const fdb5::DaosArrayName& name) : name_(name), open_(false), offset_(0) {}

DaosArrayHandle::~DaosArrayHandle() {

    if (open_) {
        eckit::Log::error() << "DaosArrayHandle not closed before destruction." << std::endl;
    }
}

void DaosArrayHandle::print(std::ostream& s) const {
    s << "DaosArrayHandle[notimp]";
}

void DaosArrayHandle::openForWrite(const Length& len) {

    if (open_) {
        throw eckit::SeriousBug{"Handle already opened."};
    }

    session();

    fdb5::DaosPool& p = session_->getPool(name_.poolName());
    fdb5::DaosContainer& c = p.ensureContainer(name_.containerName());

    /// @note: to open/create an array without generating a snapshot, we must:
    ///   - attempt array open and check if rc is 0 or DER_NONEXIST (DaosArray(session, name))
    ///   - attempt array create and check if rc is 0 or DER_EXIST (c.createArray(oid))
    ///   we do the latter first because it is the most likely to succeed.
    ///   If the operation fails, the DAOS client will generate an ERR log and usually persist
    ///   it to a log file (potential performance impact). So we want to have as few of these
    ///   failures as possible.
    /// @todo: implement DaosContainer::ensureArray, which attempts createArray with a catch+dismiss?
    /// @todo: have dummy daos_create_array return DER_EXIST where relevant.
    ///   This would probably require breaking transactionality of dummy
    ///   daos_create_array, and/or hit its performance.
    try {
        arr_.emplace(c.createArray(name_.OID()));
    }
    catch (fdb5::DaosEntityAlreadyExistsException& e) {
        arr_.emplace(session_.value(), name_);
    }

    arr_->open();

    /// @todo: should wipe object content?

    open_ = true;

    offset_ = eckit::Offset(0);
}

/// @note: the array size is retrieved here and ::read. For a more optimised reading
///   if the size is known in advance, see DaosArrayPartHandle.
Length DaosArrayHandle::openForRead() {

    if (open_) {
        throw eckit::SeriousBug{"Handle already opened."};
    }

    session();

    arr_.emplace(session_.value(), name_);

    arr_->open();

    open_ = true;

    return size();
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

    /// @note: if the buffer is oversized, daos does not return the actual smaller size read,
    ///   so it is calculated here and returned to the user as expected
    eckit::Length s = size();
    if (len > s - offset_) {
        read = s - offset_;
    }

    offset_ += read;

    return read;
}

void DaosArrayHandle::close() {

    if (!open_) {
        return;
    }

    arr_->close();

    open_ = false;
}

void DaosArrayHandle::flush() {

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

std::string DaosArrayHandle::title() const {

    return name_.asString();
}

fdb5::DaosSession& DaosArrayHandle::session() {

    if (!session_.has_value()) {
        session_.emplace();
    }
    return session_.value();
}

}  // namespace fdb5
