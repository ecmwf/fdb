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
#include <iomanip>

#include "eckit/exception/Exceptions.h"
#include "eckit/utils/Translator.h"
// #include "eckit/config/Resource.h"

#include "fdb5/daos/DaosSession.h"
#include "fdb5/daos/DaosContainer.h"
#include "fdb5/daos/DaosObject.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

daos_obj_id_t str_to_oid(const std::string& s) {
    ASSERT(s.length() == 33);
    ASSERT(s[16] == '.');
    ASSERT(std::all_of(s.begin(), s.begin()+16, ::isxdigit));
    ASSERT(std::all_of(s.end()-16, s.end(), ::isxdigit));

    eckit::Translator<std::string, unsigned long long> stoull;
    return {stoull(s.substr(0, 16)), stoull(s.substr(17, 16))};
}

std::string oid_to_str(const daos_obj_id_t& oid) {
    std::stringstream os;
    os << std::setw(16) << std::setfill('0') << std::hex << oid.hi << ".";
    os << std::setw(16) << std::setfill('0') << std::hex << oid.lo;
    return os.str();
}

fdb5::DaosContainer& name_to_cont_ref(fdb5::DaosSession& session, const fdb5::DaosName& name) {

    uuid_t uuid = {0};

    fdb5::DaosPool* pool;
    if (uuid_parse(name.poolName().c_str(), uuid) == 0) {
        pool = &(session.declarePool(uuid));
    } else {
        pool = &(session.declarePool(name.poolName()));
    }

    if (uuid_parse(name.contName().c_str(), uuid) == 0) {
        return pool->declareContainer(uuid);
    } else {
        return pool->declareContainer(name.contName());
    }
    
}

DaosObject::DaosObject(fdb5::DaosContainer& cont, daos_obj_id_t oid) : cont_(cont), oid_(oid), open_(false) {}

DaosObject::DaosObject(fdb5::DaosContainer& cont, const std::string& oid) : cont_(cont), open_(false) {

    oid_ = str_to_oid(oid);

}

DaosObject::DaosObject(fdb5::DaosSession& session, const fdb5::DaosName& name) : cont_(name_to_cont_ref(session, name)), open_(false) {

    oid_ = str_to_oid(name.oid());

}

DaosObject::DaosObject(fdb5::DaosSession& session, const eckit::URI& uri) : DaosObject(session, DaosName(uri)) {}

DaosObject::~DaosObject() {

    if (open_) close();

}

void DaosObject::create() {

    // TODO: is this shortcut too much? See DaosContainer::create()
    if (open_) return;

    cont_.create();

    const daos_handle_t& coh = cont_.getOpenHandle();

    DAOS_CALL(daos_array_create(coh, oid_, DAOS_TX_NONE, DaosSession::default_object_create_cell_size, DaosSession::default_object_create_chunk_size, &oh_, NULL));

    open_ = true;

}

void DaosObject::destroy() {

    NOTIMP;

}

void DaosObject::open() {

    if (open_) return;

    daos_size_t cell_size, csize;
    const daos_handle_t& coh = cont_.getOpenHandle();
    DAOS_CALL(daos_array_open(coh, oid_, DAOS_TX_NONE, DAOS_OO_RW, &cell_size, &csize, &oh_, NULL));
    
    open_ = true;

}

void DaosObject::close() {

    if (!open_) {
        eckit::Log::warning() << "Closing DaosObject " << name().asString() << ", object is not open" << std::endl;
        return;
    }

    DAOS_CALL(daos_array_close(oh_, NULL));
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

fdb5::DaosName DaosObject::name() const {

    return fdb5::DaosName(cont_.getPool().name(), cont_.name(), oid_to_str(oid_));

}

eckit::URI DaosObject::URI() const {

    return name().URI();

}

fdb5::DaosContainer& DaosObject::getContainer() const {

    return cont_;

}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5