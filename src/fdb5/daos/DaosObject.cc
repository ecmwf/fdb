/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <utility>

#include "eckit/exception/Exceptions.h"
#include "eckit/io/Buffer.h"
#include "eckit/utils/Literals.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/daos/DaosContainer.h"
#include "fdb5/daos/DaosException.h"
#include "fdb5/daos/DaosName.h"
#include "fdb5/daos/DaosObject.h"
#include "fdb5/daos/DaosSession.h"

using namespace eckit::literals;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosObject::DaosObject(fdb5::DaosContainer& cont, const fdb5::DaosOID& oid) : cont_(cont), oid_(oid), open_(false) {

    oid_.generateReservedBits(cont);
}

DaosObject::DaosObject(DaosObject&& other) noexcept :
    cont_(other.cont_), oid_(std::move(other.oid_)), oh_(std::move(other.oh_)), open_(other.open_) {

    other.open_ = false;
}

std::string DaosObject::name() const {

    return oid_.asString();
}

const fdb5::DaosOID& DaosObject::OID() const {

    return oid_;
}

eckit::URI DaosObject::URI() const {

    return fdb5::DaosName(*this).URI();
}

fdb5::DaosContainer& DaosObject::getContainer() const {

    return cont_;
}

fdb5::DaosContainer& name_to_cont_ref(fdb5::DaosSession& session, const fdb5::DaosNameBase& name) {

    fdb5::UUID uuid;

    fdb5::DaosPool* pool;
    if (uuid_parse(name.poolName().c_str(), uuid.internal) == 0) {
        pool = &(session.getPool(uuid, name.poolName()));
    }
    else {
        pool = &(session.getPool(name.poolName()));
    }

    return pool->getContainer(name.containerName());
}

DaosArray::DaosArray(DaosArray&& other) noexcept : DaosObject::DaosObject(std::move(other)) {}

DaosArray::DaosArray(fdb5::DaosContainer& cont, const fdb5::DaosOID& oid, bool verify) : DaosObject(cont, oid) {

    ASSERT(oid_.otype() == DAOS_OT_ARRAY || oid_.otype() == DAOS_OT_ARRAY_BYTE);

    if (verify && !exists()) {
        throw fdb5::DaosEntityNotFoundException("Array with oid " + oid.asString() + " not found", Here());
    }
}

DaosArray::DaosArray(fdb5::DaosContainer& cont, const fdb5::DaosOID& oid) : DaosArray(cont, oid, true) {}

DaosArray::DaosArray(fdb5::DaosSession& session, const fdb5::DaosNameBase& name) :
    DaosArray(name_to_cont_ref(session, name), name.OID()) {}

DaosArray::DaosArray(fdb5::DaosSession& session, const eckit::URI& uri) : DaosArray(session, DaosName(uri)) {}

DaosArray::~DaosArray() {

    if (open_) {
        close();
    }
}

/// @note: non-existing arrays (DAOS_OT_ARRAY) and byte-arrays
///   (DAOS_OT_ARRAY_BYTE) are reported as existing with the current approach.
/// @todo: implement proper exist check for DAOS_OT_ARRAY_BYTE.
bool DaosArray::exists() {

    try {

        open();
    }
    catch (fdb5::DaosEntityNotFoundException& e) {

        return false;
    }

    return true;
}

void DaosArray::create() {

    if (open_) {
        throw eckit::SeriousBug("Attempted create() on an open DaosArray");
    }

    if (oid_.otype() == DAOS_OT_ARRAY_BYTE) {
        return open();
    }

    const daos_handle_t& coh = cont_.getOpenHandle();

    DAOS_CALL(daos_array_create(coh, oid_.asDaosObjIdT(), DAOS_TX_NONE, fdb5::DaosSession().objectCreateCellSize(),
                                fdb5::DaosSession().objectCreateChunkSize(), &oh_, NULL));

    open_ = true;
}

void DaosArray::destroy() {

    open();

    DAOS_CALL(daos_array_destroy(oh_, DAOS_TX_NONE, NULL));

    close();
}

void DaosArray::open() {

    if (open_) {
        return;
    }

    const daos_handle_t& coh = cont_.getOpenHandle();

    if (oid_.otype() == DAOS_OT_ARRAY_BYTE) {

        /// @note: when using daos_array_open_with_attr to "create" an array,
        ///   cell and chunk size must be specified. When opening it for
        ///   subsequent reads, the same values have to be provided for consistent
        ///   access to the written data.
        ///   In principle these values should be constant, so
        ///   inconsistencies should never happen. However if these values are
        ///   going to be reconfigured, they should be recorded in the database
        ///   on creation, and read back on database access.
        DAOS_CALL(daos_array_open_with_attr(coh, oid_.asDaosObjIdT(), DAOS_TX_NONE, DAOS_OO_RW,
                                            fdb5::DaosSession().objectCreateCellSize(),
                                            fdb5::DaosSession().objectCreateChunkSize(), &oh_, NULL));
    }
    else {

        daos_size_t cell_size, csize;

        DAOS_CALL(daos_array_open(coh, oid_.asDaosObjIdT(), DAOS_TX_NONE, DAOS_OO_RW, &cell_size, &csize, &oh_, NULL));
    }

    open_ = true;
}

void DaosArray::close() {

    if (!open_) {
        eckit::Log::warning() << "Closing DaosArray " << name() << ", object is not open" << std::endl;
        return;
    }

    LOG_DEBUG_LIB(LibFdb5) << "DAOS_CALL => daos_array_close()" << std::endl;

    int code = daos_array_close(oh_, NULL);

    if (code < 0) {
        eckit::Log::warning() << "DAOS error in call to daos_array_close(), file " << __FILE__ << ", line " << __LINE__
                              << ", function " << __func__ << " [" << code << "] (" << code << ")" << std::endl;
    }

    LOG_DEBUG_LIB(LibFdb5) << "DAOS_CALL <= daos_array_close()" << std::endl;

    open_ = false;
}

/// @note: a buffer (buf) and its length (len) must be provided. A full write of
///        the buffer into the DAOS array will be attempted.
/// @note: daos_array_write fails if len to write is too large.
///        DaosArray::write therefore always returns a value equal to the provided
///        len if it succeeds (i.e. if no exception is thrown).
uint64_t DaosArray::write(const void* buf, const uint64_t& len, const eckit::Offset& off) {

    open();

    daos_array_iod_t iod;
    daos_range_t rg;

    d_sg_list_t sgl;
    d_iov_t iov;

    iod.arr_nr = 1;
    rg.rg_len = (daos_size_t)len;
    rg.rg_idx = (daos_off_t)off;
    iod.arr_rgs = &rg;

    sgl.sg_nr = 1;
    d_iov_set(&iov, (void*)buf, (size_t)len);
    sgl.sg_iovs = &iov;

    DAOS_CALL(daos_array_write(oh_, DAOS_TX_NONE, &iod, &sgl, NULL));

    return len;
}

/// @note: a buffer (buf) and its length (len) must be provided. A read from the
///        DAOS array of the full buffer length will be attempted.
/// @note: daos_array_read does not fail if requested len is larger than object,
///        and does not report back the actual amount of bytes read. The user must
///        call DaosArray::size() (i.e. daos_array_get_size) first and request
///        read of a correspondingly sized buffer.
/// @todo: implement the note above in dummy DAOS.
/// @note: therefore, this method won't be compatible with the DataHandle::read
///        interface (which is expected to return actual read bytes) unless the
///        implementation of such method checks the actual size and returns it
///        if smaller than the provided buffer - which would entail an additional
///        RPC and reduce performance. If the read interface is not properly
///        implemented, at least DataHandle::copyTo breaks.
/// @todo: fix this issue by using the "short_read" feature in daos_array_read
///        and returning actual read size, rather han having DataHandle::read
///        check the size.
/// @note: see DaosArrayPartHandle for cases where the object size is known.
uint64_t DaosArray::read(void* buf, uint64_t len, const eckit::Offset& off) {

    open();

    daos_array_iod_t iod;
    daos_range_t rg;

    d_sg_list_t sgl;
    d_iov_t iov;

    iod.arr_nr = 1;
    rg.rg_len = len;
    rg.rg_idx = (daos_off_t)off;
    iod.arr_rgs = &rg;

    sgl.sg_nr = 1;
    d_iov_set(&iov, buf, (size_t)len);
    sgl.sg_iovs = &iov;

    iod.arr_nr_short_read = 1;

    DAOS_CALL(daos_array_read(oh_, DAOS_TX_NONE, &iod, &sgl, NULL));

    return len;
}

uint64_t DaosArray::size() {

    open();

    uint64_t array_size;

    DAOS_CALL(daos_array_get_size(oh_, DAOS_TX_NONE, &array_size, NULL));

    return array_size;
}

DaosKeyValue::DaosKeyValue(DaosKeyValue&& other) noexcept : DaosObject::DaosObject(std::move(other)) {}

DaosKeyValue::DaosKeyValue(fdb5::DaosContainer& cont, const fdb5::DaosOID& oid, bool verify) : DaosObject(cont, oid) {

    ASSERT(oid_.otype() == type());

    if (verify && !exists()) {
        throw fdb5::DaosEntityNotFoundException("KeyValue with oid " + oid.asString() + " not found", Here());
    }
}

DaosKeyValue::DaosKeyValue(fdb5::DaosContainer& cont, const fdb5::DaosOID& oid) : DaosKeyValue(cont, oid, true) {}

DaosKeyValue::DaosKeyValue(fdb5::DaosSession& session, const fdb5::DaosNameBase& name) :
    DaosKeyValue(name_to_cont_ref(session, name), name.OID()) {}

DaosKeyValue::DaosKeyValue(fdb5::DaosSession& session, const eckit::URI& uri) : DaosKeyValue(session, DaosName(uri)) {}

DaosKeyValue::~DaosKeyValue() {

    if (open_) {
        close();
    }
}

bool DaosKeyValue::exists() {

    open();  /// @note: creates it if not exists

    return true;
}

void DaosKeyValue::create() {

    if (open_) {
        throw eckit::SeriousBug("Attempted create() on an open DaosKeyValue");
    }

    const daos_handle_t& coh = cont_.getOpenHandle();

    DAOS_CALL(daos_kv_open(coh, oid_.asDaosObjIdT(), DAOS_OO_RW, &oh_, NULL));

    open_ = true;
}

void DaosKeyValue::destroy() {

    open();

    DAOS_CALL(daos_kv_destroy(oh_, DAOS_TX_NONE, NULL));

    close();
}

void DaosKeyValue::open() {

    if (open_) {
        return;
    }

    create();
}

void DaosKeyValue::close() {

    if (!open_) {
        eckit::Log::warning() << "Closing DaosKeyValue " << name() << ", object is not open" << std::endl;
        return;
    }

    LOG_DEBUG_LIB(LibFdb5) << "DAOS_CALL => daos_obj_close()" << std::endl;

    int code = daos_obj_close(oh_, NULL);

    if (code < 0) {
        eckit::Log::warning() << "DAOS error in call to daos_obj_close(), file " << __FILE__ << ", line " << __LINE__
                              << ", function " << __func__ << " [" << code << "] (" << code << ")" << std::endl;
    }

    LOG_DEBUG_LIB(LibFdb5) << "DAOS_CALL <= daos_obj_close()" << std::endl;

    open_ = false;
}

uint64_t DaosKeyValue::size(const std::string& key) {

    open();

    uint64_t res{0};

    DAOS_CALL(daos_kv_get(oh_, DAOS_TX_NONE, 0, key.c_str(), &res, nullptr, NULL));

    return res;
}

bool DaosKeyValue::has(const std::string& key) {

    return size(key) != 0;
}

/// @note: daos_kv_put fails if written len is smaller than requested len
uint64_t DaosKeyValue::put(const std::string& key, const void* buf, const uint64_t& len) {

    open();

    DAOS_CALL(daos_kv_put(oh_, DAOS_TX_NONE, 0, key.c_str(), len, buf, NULL));

    return len;
}

/// @note: daos_kv_get fails if requested value does not fit in buffer
uint64_t DaosKeyValue::get(const std::string& key, void* buf, const uint64_t& len) {

    open();

    uint64_t res{len};

    DAOS_CALL(daos_kv_get(oh_, DAOS_TX_NONE, 0, key.c_str(), &res, buf, NULL));

    if (res == 0) {
        throw fdb5::DaosEntityNotFoundException("Key '" + key + "' not found in KeyValue with OID " + oid_.asString());
    }

    return res;
}

void DaosKeyValue::remove(const std::string& key) {

    open();

    DAOS_CALL(daos_kv_remove(oh_, DAOS_TX_NONE, 0, key.c_str(), NULL));
}

std::vector<std::string> DaosKeyValue::keys() {

    /// @todo: proper memory management
    size_t max_keys_per_rpc = 1024;  /// @todo: take from config
    std::vector<daos_key_desc_t> key_sizes(max_keys_per_rpc);

    d_sg_list_t sgl;
    d_iov_t sg_iov;
    const size_t bufsize = 1_KiB;
    eckit::Buffer list_buf{bufsize};
    d_iov_set(&sg_iov, (char*)list_buf, bufsize);
    sgl.sg_nr = 1;
    sgl.sg_nr_out = 0;
    sgl.sg_iovs = &sg_iov;
    daos_anchor_t listing_status = DAOS_ANCHOR_INIT;
    std::vector<std::string> listed_keys;

    while (!daos_anchor_is_eof(&listing_status)) {
        uint32_t nkeys_found = max_keys_per_rpc;
        int rc;
        list_buf.zero();

        DAOS_CALL(daos_kv_list(oh_, DAOS_TX_NONE, &nkeys_found, key_sizes.data(), &sgl, &listing_status, NULL));

        size_t key_start = 0;
        for (int i = 0; i < nkeys_found; i++) {
            listed_keys.push_back(std::string((char*)list_buf + key_start, key_sizes[i].kd_key_len));
            key_start += key_sizes[i].kd_key_len;
        }
    }
    return listed_keys;
}

eckit::MemoryStream DaosKeyValue::getMemoryStream(std::vector<char>& v, const std::string& key,
                                                  const std::string& kvTitle) {

    uint64_t sz = size(key);
    if (sz == 0) {
        throw fdb5::DaosEntityNotFoundException(std::string("Key '") + key + "' not found in " + kvTitle);
    }
    v.resize(sz);
    get(key, &v[0], sz);

    return eckit::MemoryStream(&v[0], sz);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
