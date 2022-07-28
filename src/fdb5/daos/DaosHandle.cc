/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Nicolau Manubens
///
/// @date Jul 2022

#include <daos.h>

#include "eckit/config/Resource.h"
#include "eckit/utils/Tokenizer.h"

#include "fdb5/daos/DaosHandle.h"
#include "fdb5/daos/DaosCluster.h"

using eckit::Length;
using eckit::Offset;
using eckit::URI;

namespace fdb5 {

void DaosHandle::construct(std::string& title) {

    static const std::string defaultDaosPool = eckit::Resource<std::string>("defaultDaosPool", "default");

    eckit::Tokenizer parse(":");

    std::vector<std::string> bits;
    // pool:cont:oidhi.oidlo
    parse(title, bits);

    ASSERT(bits.size() == 1 || bits.size() == 2 || bits.size() == 3);

    bool known_oid = true;
    daos_obj_id_t oid;
    bool last_is_oid;
    std::string pool_label;
    std::string cont_label;

    if (bits.size() == 1) {

        pool_label = defaultDaosPool;
        cont_label = title;
        known_oid = false;

    } else if (bits.size() == 2) {

        last_is_oid = strToOid(bits[1], &oid);

        if (last_is_oid) {

            pool_label = defaultDaosPool;
            cont_label = bits[0];

        } else {

            pool_label = bits[0];
            cont_label = bits[1];
            known_oid = false;

        }

    } else {

        pool_label = bits[0];
        cont_label = bits[1];
        ASSERT(strToOid(bits[2], &oid));

    }

    uuid_t pool_uuid;
    if (uuid_parse(pool_label.c_str(), pool_uuid) == 0) {

        pool_ = new DaosPool(pool_uuid);

    } else {

        pool_ = new DaosPool(pool_label);

    }

    // TODO: here DaosCluster::instance() should be called to initialize DAOS.
    //       At the moment it is only called upon obj_->create() below (getNextOid).
    pool_->connect();

    cont_ = pool_->declareContainer(cont_label);

    if (!known_oid) {
        
        obj_ = cont_->declareObject();
    
    } else {

        obj_ = cont_->declareObject(oid);

    }

}

DaosHandle::DaosHandle(std::string title) : open_(false), offset_(0) {

    construct(title);

}

DaosHandle::DaosHandle(std::string pool, std::string cont) : open_(false), offset_(0) {

    std::string title = pool + ":" + cont;
    construct(title);

}

DaosHandle::DaosHandle(std::string pool, std::string cont, std::string oid) : open_(false), offset_(0) {

    std::string title = pool + ":" + cont + ":" + oid;
    construct(title);

}

DaosHandle::DaosHandle(URI uri) : open_(false), offset_(0) {

    std::string title = uri.name();
    construct(title);

}

DaosHandle::~DaosHandle() {

    if (open_) close();
    delete obj_;
    delete cont_;
    delete pool_;

}

void DaosHandle::openForWrite(const Length& len) {

    if (open_) NOTIMP;

    offset_ = eckit::Offset(0);

    pool_->connect();

    // TODO: improve this
    cont_->create();
    cont_->open();

    obj_->create();

    open_ = true;

}

void DaosHandle::openForAppend(const Length& len) {

    if (open_) NOTIMP;

    pool_->connect();

    // TODO: should this opener crash if object not exists?
    cont_->create();
    cont_->open();

    obj_->create();

    open_ = true;

}

Length DaosHandle::openForRead() {

    if (open_) NOTIMP;

    offset_ = eckit::Offset(0);

    pool_->connect();

    cont_->open();

    obj_->open();

    open_ = true;

    return Length(obj_->getSize());

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

    cont_->close();

    pool_->disconnect();

    open_ = false;

}

void DaosHandle::flush() {

    // empty implmenetation

}

Length DaosHandle::size() {

    ASSERT(open_);
    return Length(obj_->getSize());

}

Length DaosHandle::estimate() {

    return size();

}

Offset DaosHandle::position() {

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
    
    return pool_->name() + ':' + cont_->name() + ':' + obj_->name();

}

void DaosHandle::print(std::ostream& s) const {
    s << "DaosHandle[notimp]";
}

}  // namespace fdb5