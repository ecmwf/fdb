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

#include "fdb5/daos/DaosPool.h"

#include "eckit/exception/Exceptions.h"

#include "fdb5/daos/DaosCluster.h"

namespace fdb5 {

DaosPool::DaosPool() : known_uuid_(false), connected_(false) {}

DaosPool::DaosPool(std::string pool_label) : known_uuid_(false), label_(pool_label), connected_(false) {}

DaosPool::DaosPool(uuid_t pool_uuid) : known_uuid_(true), connected_(false) {

    uuid_copy(uuid_, pool_uuid);

}

// TODO: cpp uuid wrapper?
DaosPool::DaosPool(std::string pool_label, uuid_t pool_uuid) : known_uuid_(true), label_(pool_label), connected_(false) {

    uuid_copy(uuid_, pool_uuid);

}

// TODO: remove exceptions from destructors.
// put a warning if connected_
// check DataHandle for ideas. AutoClose?
// possibly rename connect/disconnect to open/close?
DaosPool::~DaosPool() {

    if (connected_) disconnect();

}

void DaosPool::create() {

    // TODO: change all pre-condition checks to ASSERTs
    if (connected_) throw eckit::Exception("Cannot create a connected pool.");
    if (known_uuid_) throw eckit::Exception("Cannot create a pool with a user-specified UUID.");

    // TODO: ensure deallocation. Either try catch or make a wrapper.
    // not application code. Library code. Shared resources. Need to handle as cleanly as possible.
    // TODO: rename where possible to make less obscure
    d_rank_list_t svcl;
    svcl.rl_nr = 1;
    D_ALLOC_ARRAY(svcl.rl_ranks, svcl.rl_nr);
    ASSERT(svcl.rl_ranks);

    daos_prop_t *prop = NULL;

    if (label_.size() > 0) {

        // TODO: Ensure freeing. default_delete.
        // Throw exception if that fails.
        prop = daos_prop_alloc(1);
        prop->dpp_entries[0].dpe_type = DAOS_PROP_PO_LABEL;
        D_STRNDUP(prop->dpp_entries[0].dpe_str, label_.c_str(), DAOS_PROP_LABEL_MAX_LEN);      

    }

    // TODO: add parameters or resources to adjust these fixed values
    DAOS_CALL(dmg_pool_create(NULL, geteuid(), getegid(), NULL, NULL, default_create_scm_size, default_create_nvme_size, prop, &svcl, uuid_));

    // TODO: query the pool to ensure it's ready

    if (prop != NULL) daos_prop_free(prop);

    D_FREE(svcl.rl_ranks);

    known_uuid_ = true;

}

void DaosPool::destroy() {

    if (!known_uuid_) NOTIMP;
    if (connected_) disconnect();

    DAOS_CALL(dmg_pool_destroy(NULL, uuid_, NULL, default_destroy_force));
    
    known_uuid_ = false;

}

void DaosPool::connect() {

    if (connected_) return;

    if (!known_uuid_ && label_.size() == 0) throw eckit::Exception("Cannot attempt connecting to an unidentified pool. Either create it or provide a label upon construction.");

    if (label_.size() > 0) {

        DAOS_CALL(daos_pool_connect(label_.c_str(), NULL, DAOS_PC_RW, &poh_, NULL, NULL));

    } else {

        DAOS_CALL(daos_pool_connect(uuid_, NULL, DAOS_PC_RW, &poh_, NULL, NULL));

    }
    
    connected_ = true;

}

void DaosPool::disconnect() {

    if (!connected_) {
        eckit::Log::warning() << "Disconnecting DaosPool " << name() << ", pool is not open" << std::endl;
        return;
    }

    DAOS_CALL(daos_pool_disconnect(poh_, NULL));
    connected_ = false;

}

std::string DaosPool::name() {

    if (label_.size() > 0) return label_;

    if (!known_uuid_) throw eckit::Exception("Cannot generate a name for an unidentified pool. Either create it or provide a label upon construction.");

    char name_cstr[37];
    uuid_unparse(uuid_, name_cstr);
    return std::string(name_cstr);

}

daos_handle_t& DaosPool::getHandle() {
    
    if (connected_) return poh_;
    throw eckit::Exception("Cannot get handle of a unconnected pool.");

};

fdb5::DaosContainer* DaosPool::declareContainer() {

    return new fdb5::DaosContainer(this);

}

fdb5::DaosContainer* DaosPool::declareContainer(uuid_t cont_uuid) {

    return new fdb5::DaosContainer(this, cont_uuid);

}

fdb5::DaosContainer* DaosPool::declareContainer(std::string cont_label) {

    return new fdb5::DaosContainer(this, cont_label);

}

}  // namespace fdb5