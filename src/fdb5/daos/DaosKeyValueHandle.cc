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

#include "fdb5/daos/DaosContainer.h"
#include "fdb5/daos/DaosException.h"
#include "fdb5/daos/DaosKeyValueHandle.h"
#include "fdb5/daos/DaosObject.h"
#include "fdb5/daos/DaosPool.h"
#include "fdb5/daos/DaosSession.h"

using eckit::Length;
using eckit::Offset;

namespace fdb5 {

DaosKeyValueHandle::DaosKeyValueHandle(const fdb5::DaosKeyValueName& name, const std::string& key) :
    name_(name), key_(key), open_(false), offset_(0) {}

DaosKeyValueHandle::~DaosKeyValueHandle() {

    if (open_) {
        eckit::Log::error() << "DaosKeyValueHandle not closed before destruction." << std::endl;
    }
}

void DaosKeyValueHandle::print(std::ostream& s) const {
    s << "DaosKeyValueHandle[notimp]";
}

void DaosKeyValueHandle::openForWrite(const Length& len) {

    if (open_) {
        throw eckit::SeriousBug{"Handle already opened."};
    }

    session();

    /// @todo: alternatively call name_.create() and the like
    fdb5::DaosPool& p = session_->getPool(name_.poolName());
    fdb5::DaosContainer& c = p.ensureContainer(name_.containerName());

    /// @note: only way to check kv existence without generating a snapshot is
    ///   to attempt open, which results in creation without an error rc if n.e.
    ///   A kv open is performed in both c.createKeyValue and DaosKeyValue(session, name).
    ///   The former is used here.
    kv_.emplace(c.createKeyValue(name_.OID()));

    kv_->open();

    open_ = true;

    offset_ = eckit::Offset(0);
}

Length DaosKeyValueHandle::openForRead() {

    if (open_) {
        throw eckit::SeriousBug{"Handle already opened."};
    }

    session();

    kv_.emplace(session_.value(), name_);

    kv_->open();

    open_ = true;

    offset_ = eckit::Offset(0);

    return Length(kv_->size(key_));
}

long DaosKeyValueHandle::write(const void* buf, long len) {

    ASSERT(open_);

    long written = kv_->put(key_, buf, len);

    offset_ += written;

    return written;
}

long DaosKeyValueHandle::read(void* buf, long len) {

    ASSERT(open_);

    long read = kv_->get(key_, buf, len);

    offset_ += read;

    return read;
}

void DaosKeyValueHandle::close() {

    if (!open_) {
        return;
    }

    kv_->close();

    open_ = false;
}

void DaosKeyValueHandle::flush() {

    /// empty implmenetation
}

Length DaosKeyValueHandle::size() {

    fdb5::DaosKeyValue kv{session(), name_};

    return Length(kv.size(key_));
}

Length DaosKeyValueHandle::estimate() {

    return size();
}

Offset DaosKeyValueHandle::position() {

    /// @todo: should position() crash if unopened?
    return offset_;
}

bool DaosKeyValueHandle::canSeek() const {

    return false;
}

std::string DaosKeyValueHandle::title() const {

    return name_.asString();
}

fdb5::DaosSession& DaosKeyValueHandle::session() {

    if (!session_.has_value()) {
        session_.emplace();
    }
    return session_.value();
}

}  // namespace fdb5
