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

#include "fdb5/daos/DaosArrayPartHandle.h"
#include "fdb5/daos/DaosContainer.h"
#include "fdb5/daos/DaosException.h"
#include "fdb5/daos/DaosObject.h"
#include "fdb5/daos/DaosPool.h"
#include "fdb5/daos/DaosSession.h"

using eckit::Length;
using eckit::Offset;

namespace fdb5 {

DaosArrayPartHandle::DaosArrayPartHandle(const fdb5::DaosArrayName& name, const eckit::Offset& off,
                                         const eckit::Length& len) :
    name_(name), open_(false), offset_(off), len_(len) {}

DaosArrayPartHandle::~DaosArrayPartHandle() {

    if (open_) {
        eckit::Log::error() << "DaosArrayPartHandle not closed before destruction." << std::endl;
    }
}

void DaosArrayPartHandle::print(std::ostream& s) const {
    s << "DaosArrayPartHandle[notimp]";
}

Length DaosArrayPartHandle::openForRead() {

    if (open_) {
        throw eckit::SeriousBug{"Handle already opened."};
    }

    session();

    arr_.emplace(session_.value(), name_);

    arr_->open();

    open_ = true;

    return size();
}

long DaosArrayPartHandle::read(void* buf, long len) {

    ASSERT(open_);

    /// @note: if the buffer is oversized, daos does not return the actual smaller size read,
    ///   so it is calculated here and returned to the user as expected
    eckit::Length s = size();
    if (len > s - offset_) {
        len = s - offset_;
    }

    long read = arr_->read(buf, len, offset_);

    offset_ += read;

    return read;
}

void DaosArrayPartHandle::close() {

    if (!open_) {
        return;
    }

    arr_->close();

    open_ = false;
}

void DaosArrayPartHandle::flush() {

    /// empty implmenetation
}

Length DaosArrayPartHandle::size() {

    return len_;
}

Length DaosArrayPartHandle::estimate() {

    return size();
}

Offset DaosArrayPartHandle::position() {

    /// @todo: should position() crash if unopened?
    return offset_;
}

Offset DaosArrayPartHandle::seek(const Offset& offset) {

    offset_ = offset;
    /// @todo: assert offset <= size() ?
    return offset_;
}

bool DaosArrayPartHandle::canSeek() const {

    return true;
}

// void DaosArrayHandle::skip(const Length& len) {

//     offset_ += Offset(len);

// }

std::string DaosArrayPartHandle::title() const {

    return name_.asString();
}

fdb5::DaosSession& DaosArrayPartHandle::session() {

    if (!session_.has_value()) {
        session_.emplace();
    }
    return session_.value();
}

}  // namespace fdb5
