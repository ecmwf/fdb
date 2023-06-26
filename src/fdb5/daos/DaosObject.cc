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

DaosObject::DaosObject(fdb5::DaosContainer& cont, const fdb5::DaosOID& oid) : cont_(cont), oid_(oid), open_(false) {}

DaosObject::DaosObject(DaosObject&& other) noexcept : cont_(other.cont_), oid_(std::move(other.oid_)), 
    oh_(std::move(other.oh_)), open_(other.open_) {

    other.open_ = false;

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

fdb5::DaosContainer& name_to_cont_ref(fdb5::DaosSession& session, const fdb5::DaosNameBase& name) {

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

DaosArray::DaosArray(DaosArray&& other) noexcept : DaosObject::DaosObject(std::move(other)) {}

DaosArray::DaosArray(fdb5::DaosContainer& cont, const fdb5::DaosOID& oid, bool verify) : DaosObject(cont, oid) {

    ASSERT(oid_.wasGenerated());
    ASSERT(oid_.otype() == type());

    if (verify && !exists()) {
        throw fdb5::DaosEntityNotFoundException(
            "Array with oid " + oid.asString() + " not found", 
            Here());
    }
    
}

DaosArray::DaosArray(fdb5::DaosContainer& cont, const fdb5::DaosOID& oid) : DaosArray(cont, oid, true) {}

DaosArray::DaosArray(fdb5::DaosSession& session, const fdb5::DaosNameBase& name) : 
    DaosArray(name_to_cont_ref(session, name), name.OID()) {}

DaosArray::DaosArray(fdb5::DaosSession& session, const eckit::URI& uri) : DaosArray(session, DaosName(uri)) {}

DaosArray::~DaosArray() {

    if (open_) close();

}

bool DaosArray::exists() {

    /// @todo: implement this with more appropriate DAOS API functions
    try {

        open();

    } catch (eckit::SeriousBug& e) {

        return false;
        
    }
    
    return true;

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
        << code << ")" << std::endl;
        
    std::cout << "DAOS_CALL <= daos_array_close()" << std::endl;

    open_ = false;

}

// TODO: why are len parameters in eckit::DataHandle not const references?
/// @note: a buffer (buf) and its length (len) must be provided. A full write of 
///        the buffer into the DAOS array will be attempted.
/// @note: daos_array_write fails if len to write is too large.
///        DaosArray::write therefore always returns a value equal to the provided
///        len if it succeeds (i.e. if no exception is thrown).
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

/// @note: a buffer (buf) and its length (len) must be provided. A read from the  
///        DAOS array of the full buffer length will be attempted.
/// @note: daos_array_read fails if requested len is larger than object.
///        DaosArray::read therefore always returns a value equal to the provided
///        len if it succeeds (i.e. if no exception is thrown).
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

DaosKeyValue::DaosKeyValue(DaosKeyValue&& other) noexcept : DaosObject::DaosObject(std::move(other)) {}

DaosKeyValue::DaosKeyValue(fdb5::DaosContainer& cont, const fdb5::DaosOID& oid, bool verify) : DaosObject(cont, oid) {

    ASSERT(oid_.wasGenerated());
    ASSERT(oid_.otype() == type());

    if (verify && !exists()) {
        throw fdb5::DaosEntityNotFoundException(
            "KeyValue with oid " + oid.asString() + " not found", 
            Here());
    }
    
}

DaosKeyValue::DaosKeyValue(fdb5::DaosContainer& cont, const fdb5::DaosOID& oid) : DaosKeyValue(cont, oid, true) {}

DaosKeyValue::DaosKeyValue(fdb5::DaosSession& session, const fdb5::DaosNameBase& name) : 
    DaosKeyValue(name_to_cont_ref(session, name), name.OID()) {}

DaosKeyValue::DaosKeyValue(fdb5::DaosSession& session, const eckit::URI& uri) : DaosKeyValue(session, DaosName(uri)) {}

DaosKeyValue::~DaosKeyValue() {

    if (open_) close();

}

bool DaosKeyValue::exists() {

    /// @todo: implement this with more appropriate DAOS API functions
    try {

        open();

    } catch (eckit::SeriousBug& e) {

        return false;
        
    }
    
    return true;

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
        << code << ")" << std::endl;
        
    std::cout << "DAOS_CALL <= daos_obj_close()" << std::endl;

    open_ = false;

}

daos_size_t DaosKeyValue::size(const std::string& key) {

    open();

    long res{0};
    DAOS_CALL(daos_kv_get(oh_, DAOS_TX_NONE, 0, key.c_str(), (daos_size_t*) &res, nullptr, NULL));

    return res;

}

bool DaosKeyValue::has(const std::string& key) {

    return size(key) != 0;

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

std::vector<std::string> DaosKeyValue::keys() {

    /// @todo: proper memory management
    int max_keys_per_rpc = 1024;  /// @todo: take from config
    daos_key_desc_t key_sizes[max_keys_per_rpc];
    d_sg_list_t sgl;
    d_iov_t sg_iov;
    char *list_buf;
    int bufsize = 1024;
    list_buf = (char*) malloc(bufsize);
    d_iov_set(&sg_iov, list_buf, bufsize);
    sgl.sg_nr = 1;
    sgl.sg_nr_out = 0;
    sgl.sg_iovs = &sg_iov;
    daos_anchor_t listing_status = DAOS_ANCHOR_INIT;
    std::vector<std::string> listed_keys;
    while (!daos_anchor_is_eof(&listing_status)) {
        uint32_t nkeys_found = max_keys_per_rpc;
        int rc;
        memset(list_buf, 0, bufsize);
        DAOS_CALL(daos_kv_list(oh_, DAOS_TX_NONE, &nkeys_found, key_sizes, &sgl, &listing_status, NULL));
        size_t key_start = 0;
        for (int i = 0; i < nkeys_found; i++) {
            listed_keys.push_back(std::string(list_buf + key_start, key_sizes[i].kd_key_len));
            key_start += key_sizes[i].kd_key_len;
        }
    }
    return listed_keys;

}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5