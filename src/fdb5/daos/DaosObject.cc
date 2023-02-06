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

#include "fdb5/daos/DaosSession.h"
#include "fdb5/daos/DaosContainer.h"
#include "fdb5/daos/DaosObject.h"
#include "fdb5/daos/DaosName.h"
#include "fdb5/daos/DaosException.h"

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

DaosObject::DaosObject(fdb5::DaosContainer& cont, const fdb5::DaosOID& oid, bool verify) : 
    cont_(cont), oid_(oid), open_(false) {

    ASSERT(oid_.wasGenerated());

    if (verify && !exists()) {
        throw fdb5::DaosEntityNotFoundException(
            "Object with oid " + oid.asString() + " not found", 
            Here());
    }
    
}

DaosObject::DaosObject(fdb5::DaosContainer& cont, const fdb5::DaosOID& oid) : DaosObject(cont, oid, true) {}

DaosObject::DaosObject(fdb5::DaosSession& session, const fdb5::DaosName& name) : 
    DaosObject(name_to_cont_ref(session, name), name.OID()) {}

DaosObject::DaosObject(fdb5::DaosSession& session, const eckit::URI& uri) : DaosObject(session, DaosName(uri)) {}

DaosObject::DaosObject(DaosObject&& other) noexcept : cont_(other.cont_), open_(other.open_) {

    std::swap(oid_, other.oid_);
    std::swap(oh_, other.oh_);
    other.open_ = false;

}

const daos_handle_t& DaosObject::getOpenHandle() {
    
    open();
    return oh_;
    
};

bool DaosObject::exists() {

    /// @todo: implement this with more appropriate DAOS API functions
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

DaosArray::~DaosArray() {

    if (open_) close();

}

void DaosArray::create() {

    if (open_) throw eckit::SeriousBug("Attempted create() on an open DaosArray");

    const daos_handle_t& coh = cont_.getOpenHandle();

    DAOS_CALL(
        daos_array_create(
            coh, oid_.asDaosObjIdT(), DAOS_TX_NONE,
            getContainer().getPool().getSession().objectCreateCellSize(),
            getContainer().getPool().getSession().objectCreateChunkSize(),
            &oh_, NULL
        )
    );

    open_ = true;

}

void DaosArray::destroy() {

    NOTIMP;

}

void DaosArray::open() {

    if (open_) return;

    daos_size_t cell_size, csize;
    const daos_handle_t& coh = cont_.getOpenHandle();
    DAOS_CALL(daos_array_open(coh, oid_.asDaosObjIdT(), DAOS_TX_NONE, DAOS_OO_RW, &cell_size, &csize, &oh_, NULL));
    
    open_ = true;

}

void DaosArray::close() {

    if (!open_) {
        eckit::Log::warning() << "Closing DaosArray " << name() << ", object is not open" << std::endl;
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

// TODO: why are len parameters in eckit::DataHandle not const references?
/// @note: daos_array_write fails if written len is smaller than requested len
long DaosArray::write(const void* buf, const long& len, const eckit::Offset& off) {

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

/// @note: daos_array_read fails if requested len is larger than object
/// @note: daos_array_read fails if read len is smaller than requested len
long DaosArray::read(void* buf, const long& len, const eckit::Offset& off) {

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

// TODO: should return a long for consistency with the rest of DaosArray API
daos_size_t DaosArray::size() {

    open();

    daos_size_t array_size;
    DAOS_CALL(daos_array_get_size(oh_, DAOS_TX_NONE, &array_size, NULL));

    return array_size;

}

DaosKeyValue::~DaosKeyValue() {

    if (open_) close();

}

// TODO: semantics in DAOS API seem different from dummy DAOS. Real daos_kv_open does not involve RPC
void DaosKeyValue::create() {

    if (open_) throw eckit::SeriousBug("Attempted create() on an open DaosKeyValue");

    const daos_handle_t& coh = cont_.getOpenHandle();

    DAOS_CALL(daos_kv_open(coh, oid_.asDaosObjIdT(), DAOS_OO_RW, &oh_, NULL));

    open_ = true;

}

void DaosKeyValue::destroy() {

    NOTIMP;

}

void DaosKeyValue::open() {

    if (open_) return;

    create();

}

void DaosKeyValue::close() {

    if (!open_) {
        eckit::Log::warning() << "Closing DaosKeyValue " << name() << ", object is not open" << std::endl;
        return;
    }
    
    std::cout << "DAOS_CALL => daos_obj_close()" << std::endl;

    int code = daos_obj_close(oh_, NULL);

    if (code < 0) eckit::Log::warning() << "DAOS error in call to daos_obj_close(), file " 
        << __FILE__ << ", line " << __LINE__ << ", function " << __func__ << " [" << code << "] (" 
        << strerror(-code) << ")" << std::endl;
        
    std::cout << "DAOS_CALL <= daos_obj_close()" << std::endl;

    open_ = false;

}

/// @note: daos_kv_put fails if written len is smaller than requested len
long DaosKeyValue::put(const std::string& key, const void* buf, const long& len) {

    open();

    DAOS_CALL(daos_kv_put(oh_, DAOS_TX_NONE, 0, key.c_str(), (daos_size_t) len, buf, NULL));

    return len;

}

/// @note: daos_kv_get fails if requested value does not fit in buffer
long DaosKeyValue::get(const std::string& key, void* buf, const long& len) {

    open();

    long res{len};
    DAOS_CALL(daos_kv_get(oh_, DAOS_TX_NONE, 0, key.c_str(), (daos_size_t*) &res, buf, NULL));

    if (res == 0) throw fdb5::DaosEntityNotFoundException("Key '" + key + "' not found in KeyValue with OID " + oid_.asString());

    return res;

}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5