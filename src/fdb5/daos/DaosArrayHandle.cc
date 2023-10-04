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

/// @todo: develop DaosArrayPartHandle, and use it in DaosFieldLocation::dataHandle.
/// that part handle will not need to query size for openForRead, and read()ing will be performed directly

DaosArrayHandle::DaosArrayHandle(const fdb5::DaosArrayName& name) : name_(name), open_(false), offset_(0) {}

DaosArrayHandle::~DaosArrayHandle() {

    if (open_) eckit::Log::error() << "DaosArrayHandle not closed before destruction." << std::endl;

}

void DaosArrayHandle::print(std::ostream& s) const {
    s << "DaosArrayHandle[notimp]";
}

void DaosArrayHandle::openForWrite(const Length& len) {

    if (open_) NOTIMP;

    mode_ = "archive";

    using namespace std::placeholders;
    eckit::Timer& timer = fdb5::DaosManager::instance().timer();
    fdb5::DaosIOStats& stats = fdb5::DaosManager::instance().stats();

    fdb5::StatsTimer st{"archive 100 array handle ensure container", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};

    session_.reset(new fdb5::DaosSession());

    fdb5::DaosPool& p = session_->getPool(name_.poolName());
    fdb5::DaosContainer& c = p.ensureContainer(name_.contName());

    st.stop();

    st.start("archive 101 array handle array create", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));

    /// @todo: optionally remove this, as name_.OID() and OID generation are
    ///    triggered as part of DaosArray constructors.
    name_.generateOID();

    /// @note: only ways to check if array exists without generating a snapshot are:
    ///   - attempt array open and check if rc is 0 or DER_NONEXIST (DaosArray(session, name))
    ///   - attempt array create and check if rc is 0 or DER_EXIST (c.createArray(oid))
    ///   we do the latter first because it is the most likely to succeed.
    ///   If the operation fails, the DAOS client will generate an ERR log and usually persist
    ///   it to a log file (potential performance impact). So we want to have as few of these 
    ///   failures as possible.
    /// @todo: implement DaosContainer::ensureArray, which attempts createArray with a catch+dismiss?
    /// @todo: have dummy daos_create_array return DER_EXIST where relevant
    try {
        arr_.reset(new fdb5::DaosArray( c.createArray(name_.OID()) ));
    } catch (fdb5::DaosEntityAlreadyExistsException& e) {
        arr_.reset(new fdb5::DaosArray(*(session_.get()), name_));
    }

    arr_->open();

    st.stop();

    /// @todo: should wipe object content?

    open_ = true;

    offset_ = eckit::Offset(0);

}

void DaosArrayHandle::openForAppend(const Length& len) {

    /// @todo: implement openForAppend with its own code, with a slight variation
    ///   with respect to openForWrite: try open first, if failure then create
    openForWrite(len);

    /// @todo: should offset be set to size() or be left to its current value?
    offset_ = eckit::Offset(arr_->size());

}

/// @note: the array size is retrieved here and ::read. For a more optimised reading 
// if the size is known in advance, see DaosArrayPartHandle.
Length DaosArrayHandle::openForRead() {

    if (open_) NOTIMP;

    mode_ = "retrieve";

    using namespace std::placeholders;
    eckit::Timer& timer = fdb5::DaosManager::instance().timer();
    fdb5::DaosIOStats& stats = fdb5::DaosManager::instance().stats();
    fdb5::StatsTimer st{"retrieve 09 array handle array open and get size", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};

    session_.reset(new fdb5::DaosSession());

    name_.generateOID();

    arr_.reset(new fdb5::DaosArray(*(session_.get()), name_));

    arr_->open();

    open_ = true;

    return size();

}

long DaosArrayHandle::write(const void* buf, long len) {

    ASSERT(open_);

    using namespace std::placeholders;
    eckit::Timer& timer = fdb5::DaosManager::instance().timer();
    fdb5::DaosIOStats& stats = fdb5::DaosManager::instance().stats();
    fdb5::StatsTimer st{"archive 11 array handle array write", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};

    long written = arr_->write(buf, len, offset_);

    offset_ += written;

    return written;

}

long DaosArrayHandle::read(void* buf, long len) {

    ASSERT(open_);

    using namespace std::placeholders;
    eckit::Timer& timer = fdb5::DaosManager::instance().timer();
    fdb5::DaosIOStats& stats = fdb5::DaosManager::instance().stats();
    fdb5::StatsTimer st{"retrieve 10 array handle array read", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};

    long read = arr_->read(buf, len, offset_);

    /// @note: if the buffer is oversized, daos does not return the actual smaller size read,
    ///   so it is calculated here and returned to the user as expected
    eckit::Length s = size();
    if (len > s - offset_) read = s - offset_;

    offset_ += read;

    return read;

}

void DaosArrayHandle::close() {

    if (!open_) return;

    using namespace std::placeholders;
    eckit::Timer& timer = fdb5::DaosManager::instance().timer();
    fdb5::DaosIOStats& stats = fdb5::DaosManager::instance().stats();
    fdb5::StatsTimer st{mode_ + " 12 array handle array close", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};

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

// void DaosArrayHandle::skip(const Length& len) {

//     offset_ += Offset(len);

// }

std::string DaosArrayHandle::title() const {
    
    return name_.asString();

}

}  // namespace fdb5
