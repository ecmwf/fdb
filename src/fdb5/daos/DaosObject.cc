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

#include "fdb5/daos/DaosContainer.h"

#include "eckit/exception/Exceptions.h"

namespace fdb5 {

// TODO: adddress this.
DaosObject::DaosObject() {}

DaosObject::DaosObject(fdb5::DaosContainer* cont) : cont_(cont), known_oid_(false), open_(false) {}

DaosObject::DaosObject(fdb5::DaosContainer* cont, daos_obj_id_t oid) : cont_(cont), oid_(oid), known_oid_(true), open_(false) {}

DaosObject::~DaosObject() {

    if (open_) close();

}

void DaosObject::create() {

    // TODO: not sure what to do here. Should probably keep track of whether 
    //       the object has been created or not via this DaosObject instance,
    //       and return accordingly. But the object may be destroyed by another 
    //       process or DaosObject instance.
    //if (open_) return;

    if (!known_oid_) {

        oid_ = DaosCluster::instance().getNextOid(cont_);
        known_oid_ = true;

    }

    daos_handle_t coh = cont_->getHandle();

    DAOS_CALL(daos_array_create(coh, oid_, DAOS_TX_NONE, default_create_cell_size, default_create_chunk_size, &oh_, NULL));

    open_ = true;

}

void DaosObject::destroy() {

    NOTIMP;

}

void DaosObject::open() {

    if (open_) return;

    if (!known_oid_) throw eckit::Exception("Cannot attempt connecting to an unidentified object. Either create it or provide a OID upon construction.");

    daos_size_t cell_size, csize;
    daos_handle_t coh = cont_->getHandle();
    DAOS_CALL(daos_array_open(coh, oid_, DAOS_TX_NONE, DAOS_OO_RW, &cell_size, &csize, &oh_, NULL));
    
    open_ = true;

}

void DaosObject::close() {

    if (!open_) {
        eckit::Log::warning() << "Closing DaosObject " << name() << ", object is not open" << std::endl;
        return;
    }

    DAOS_CALL(daos_array_close(oh_, NULL));
    open_ = false;

}

long DaosObject::write(const void* buf, long len, eckit::Offset off) {

    ASSERT(open_);

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

    ASSERT(open_);

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

daos_size_t DaosObject::getSize() {

    daos_size_t array_size;
    DAOS_CALL(daos_array_get_size(oh_, DAOS_TX_NONE, &array_size, NULL));

    return array_size;

}

std::string DaosObject::name() {

    return oidToStr(oid_);

}

daos_handle_t& DaosObject::getHandle() {
    
    if (open_) return oh_;
    throw eckit::Exception("Cannot get handle of unopened object.");
    
};

}  // namespace fdb5