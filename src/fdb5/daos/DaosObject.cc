/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

// #include <iostream>
#include <utility>

#include "eckit/exception/Exceptions.h"
// #include "eckit/config/Resource.h"

#include "fdb5/daos/DaosSession.h"
#include "fdb5/daos/DaosContainer.h"
#include "fdb5/daos/DaosObject.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

fdb5::DaosContainer& name_to_cont_ref(fdb5::DaosSession& session, const fdb5::DaosName& name) {

    uuid_t uuid = {0};

    fdb5::DaosPool* pool;
    if (uuid_parse(name.poolName().c_str(), uuid) == 0) {
        pool = &(session.getPool(uuid));
    } else {
        pool = &(session.getPool(name.poolName()));
    }

    if (uuid_parse(name.contName().c_str(), uuid) == 0) {
        return pool->getContainer(uuid);
    } else {
        return pool->getContainer(name.contName());
    }
    
}

DaosObject::DaosObject(fdb5::DaosContainer& cont, const fdb5::DaosOID& oid, bool verify) : cont_(cont), oid_(oid), open_(false) { if (verify) ASSERT(exists()); }

DaosObject::DaosObject(fdb5::DaosContainer& cont, const fdb5::DaosOID& oid) : DaosObject(cont, oid, true) {}

DaosObject::DaosObject(fdb5::DaosSession& session, const fdb5::DaosName& name) : cont_(name_to_cont_ref(session, name)), oid_(name.OID()), open_(false) { ASSERT(exists()); }

DaosObject::DaosObject(fdb5::DaosSession& session, const eckit::URI& uri) : DaosObject(session, DaosName(uri)) {}

DaosObject::DaosObject(DaosObject&& rhs) noexcept : cont_(rhs.cont_), open_(rhs.open_) {

    std::swap(oid_, rhs.oid_);
    std::swap(oh_, rhs.oh_);
    rhs.open_ = false;

}

DaosObject::~DaosObject() {

    if (open_) close();

}

void DaosObject::create() {

    if (open_) throw eckit::SeriousBug("Attempted create() on an open DaosObject");

    const daos_handle_t& coh = cont_.getOpenHandle();

    DAOS_CALL(daos_array_create(coh, oid_.asDaosObjIdT(), DAOS_TX_NONE, DaosSession::default_object_create_cell_size, DaosSession::default_object_create_chunk_size, &oh_, NULL));

    open_ = true;

}

void DaosObject::destroy() {

    NOTIMP;

}

void DaosObject::open() {

    if (open_) return;

    daos_size_t cell_size, csize;
    const daos_handle_t& coh = cont_.getOpenHandle();
    DAOS_CALL(daos_array_open(coh, oid_.asDaosObjIdT(), DAOS_TX_NONE, DAOS_OO_RW, &cell_size, &csize, &oh_, NULL));
    
    open_ = true;

}

void DaosObject::close() {

    if (!open_) {
        eckit::Log::warning() << "Closing DaosObject " << name() << ", object is not open" << std::endl;
        return;
    }
    
    std::cout << "DAOS_CALL => daos_array_close()" << std::endl;

    int code = daos_array_close(oh_, NULL);

    if (code < 0) eckit::Log::warning() << "DAOS error in call to daos_array_close(), file " 
        << __FILE__ << ", line " << __LINE__ << ", function " << __func__ << " [" << code << "] (" 
        << strerror(-code) << ")" << std::endl;
        
    std::cout << "DAOS_CALL <= daos_array_close()" << std::endl;

    open_ = false;

}

long DaosObject::write(const void* buf, long len, eckit::Offset off) {

    open();

    daos_array_iod_t iod;
    daos_range_t rg;

    d_sg_list_t sgl;
    d_iov_t iov;

    iod.arr_nr = 1;
    rg.rg_len = (daos_size_t) len;
    rg.rg_idx = (daos_off_t) off;
    iod.arr_rgs = &rg;

    sgl.sg_nr = 1;
    d_iov_set(&iov, (void*) buf, (size_t) len);
    sgl.sg_iovs = &iov;

    DAOS_CALL(daos_array_write(oh_, DAOS_TX_NONE, &iod, &sgl, NULL));

    return len;

}

long DaosObject::read(void* buf, long len, eckit::Offset off) {

    open();

    daos_array_iod_t iod;
    daos_range_t rg;

    d_sg_list_t sgl;
    d_iov_t iov;

    iod.arr_nr = 1;
    rg.rg_len = len;
    rg.rg_idx = (daos_off_t) off;
    iod.arr_rgs = &rg;

    sgl.sg_nr = 1;
    d_iov_set(&iov, buf, (size_t) len);
    sgl.sg_iovs = &iov;

    DAOS_CALL(daos_array_read(oh_, DAOS_TX_NONE, &iod, &sgl, NULL));

    return len;

}

daos_size_t DaosObject::size() {

    open();

    daos_size_t array_size;
    DAOS_CALL(daos_array_get_size(oh_, DAOS_TX_NONE, &array_size, NULL));

    return array_size;

}

const daos_handle_t& DaosObject::getOpenHandle() {
    
    open();
    return oh_;
    
};

bool DaosObject::exists() {

    // TODO: implement this with more appropriate DAOS API functions
    try {

        open();

    } catch (eckit::SeriousBug& e) {

        return false;
        
    }
    
    return true;

}

std::string DaosObject::name() const {

    return oid_.asString();

}

fdb5::DaosOID DaosObject::OID() const {

    return oid_;

}

eckit::URI DaosObject::URI() const {

    return fdb5::DaosName(*this).URI();

}

fdb5::DaosContainer& DaosObject::getContainer() const {

    return cont_;

}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5