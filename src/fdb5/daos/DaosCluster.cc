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

#include "fdb5/daos/DaosCluster.h"

namespace fdb5 {

// struct OidAlloc {
//     OidAlloc() : num_oids_(0) {}
//     uint64_t next_oid_;
//     int num_oids_;
// };

DaosCluster& DaosCluster::instance() {
    static DaosCluster instance_;
    return instance_;
}

DaosCluster::DaosCluster() {

    DAOS_CALL(daos_init());

}

DaosCluster::~DaosCluster() {

    DAOS_CALL(daos_fini());

}

void DaosCluster::poolConnect(std::string& pool, daos_handle_t* poh) const {

    DAOS_CALL(daos_pool_connect(pool.c_str(), NULL, DAOS_PC_RW, poh, NULL, NULL));

}

void DaosCluster::poolDisconnect(daos_handle_t& poh) const {

    DAOS_CALL(daos_pool_disconnect(poh, NULL));

}

void DaosCluster::contCreateWithLabel(daos_handle_t& poh, std::string& label) const {

    DAOS_CALL(daos_cont_create_with_label(poh, label.c_str(), NULL, NULL, NULL));

}

void DaosCluster::contOpen(daos_handle_t& poh, std::string& cont, daos_handle_t *coh) const {

    DAOS_CALL(daos_cont_open(poh, cont.c_str(), DAOS_COO_RW, coh, NULL, NULL));

}

void DaosCluster::contClose(daos_handle_t& coh) const {

    DAOS_CALL(daos_cont_close(coh, NULL));

}

daos_obj_id_t DaosCluster::getNextOid(std::string& pool, std::string& cont) {

    // OidAlloc *alloc = nullptr;

    // auto j = oid_allocs_.find(pool);
    // if (j != oid_allocs_.end()) {
    //     auto k = j->second->find(cont);
    //     if (k != j->second->end())
    //         alloc = k->second;
    // }

    // if (!alloc) {
    //     oid_allocs_[pool][0][cont] = new OidAlloc();
    //     alloc = oid_allocs_[pool][0][cont];
    // }

    daos_handle_t poh, coh;
    poolConnect(pool, &poh);
    // TODO: improve this
    contCreateWithLabel(poh, cont);
    contOpen(poh, cont, &coh);

    // if (alloc->num_oids_ == 0) {
    //     alloc->num_oids_ = OIDS_PER_ALLOC;
    //     DAOS_CALL(daos_cont_alloc_oids(coh, alloc->num_oids_ + 1, &(alloc->next_oid_), NULL));
    // } else {
    //     alloc->next_oid_++;
    //     alloc->num_oids_--;
    // }

    daos_obj_id_t next;
    DAOS_CALL(daos_cont_alloc_oids(coh, (daos_size_t) 1, &(next.lo), NULL));
    DAOS_CALL(daos_array_generate_oid(coh, &next, true, OC_S1, 0, 0));

    // next.lo = alloc->next_oid_;
    // DAOS_CALL(daos_array_generate_oid(coh, &next, true, OC_S1, 0, 0));

    contClose(coh);
    poolDisconnect(poh);

    return next;
}

void DaosCluster::arrayCreate(daos_handle_t& coh, daos_obj_id_t& oid, daos_handle_t *oh) const {

    DAOS_CALL(daos_array_create(coh, oid, DAOS_TX_NONE, 1, 1048576, oh, NULL));

}

void DaosCluster::arrayOpen(daos_handle_t& coh, daos_obj_id_t& oid, daos_handle_t *oh) const {

    daos_size_t cell_size, csize;
    DAOS_CALL(daos_array_open(coh, oid, DAOS_TX_NONE, DAOS_OO_RW, &cell_size, &csize, oh, NULL));

}

daos_size_t DaosCluster::arrayGetSize(daos_handle_t& oh) const {

    daos_size_t array_size;
    DAOS_CALL(daos_array_get_size(oh, DAOS_TX_NONE, &array_size, NULL));

    return array_size;

}

void DaosCluster::arrayClose(daos_handle_t& oh) const {

    DAOS_CALL(daos_array_close(oh, NULL));

}

void DaosCluster::error(int code, const char* msg, const char* file, int line, const char* func) {

    std::ostringstream oss;
    oss << "DAOS error " << msg << ", file " << file << ", line " << line << ", function " << func << " [" << code
        << "] (" << strerror(-code) << ")";
    throw eckit::SeriousBug(oss.str());
}

}  // namespace fdb5