/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <daos/tests_lib.h>

#include "eckit/exception/Exceptions.h"

#include "fdb5/daos/DaosPool.h"
#include "fdb5/daos/DaosSession.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosPool::DaosPool(fdb5::DaosSession& session) : session_(session), known_uuid_(false), open_(false) {}

DaosPool::DaosPool(fdb5::DaosSession& session, uuid_t uuid) : session_(session), known_uuid_(true), open_(false) {

    uuid_copy(uuid_, uuid);

}

DaosPool::DaosPool(fdb5::DaosSession& session, const std::string& label) : session_(session), known_uuid_(false), label_(label), open_(false) {}


DaosPool::DaosPool(fdb5::DaosSession& session, uuid_t uuid, const std::string& label) : session_(session), known_uuid_(true), label_(label), open_(false) {

    uuid_copy(uuid_, uuid);

}

DaosPool::~DaosPool() {

    cont_cache_.clear();

    if (open_) close();

}

void DaosPool::create() {

    // "Cannot create a pool with a user-specified UUID."
    ASSERT(!known_uuid_);

    // TODO: ensure deallocation. Either try catch or make a wrapper.
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
    DAOS_CALL(dmg_pool_create(NULL, geteuid(), getegid(), NULL, NULL, DaosSession::default_pool_create_scm_size, DaosSession::default_pool_create_nvme_size, prop, &svcl, uuid_));

    // TODO: query the pool to ensure it's ready

    if (prop != NULL) daos_prop_free(prop);

    D_FREE(svcl.rl_ranks);

    known_uuid_ = true;

}

void DaosPool::destroy() {

    // TODO: this will result in a invalid DaosPool instance, we want to avoid this

    if (!known_uuid_) NOTIMP;

    session_.destroyPoolContainers(uuid_);
    session_.closePool(uuid_);

    // TODO: cached DaosPools declared with a label only, pointing to the pool
    // being destroyed may still exist and should be closed.

    DAOS_CALL(dmg_pool_destroy(NULL, uuid_, NULL, fdb5::DaosSession::default_pool_destroy_force));

    // TODO: the DaosPools pointing to the destroyed DAOS pool are now invalid
    // and cannot be reopened unless a new pool with the same label or uuid 
    // is created.
    // They could be removed from the cache and deleted but then the user would 
    // be left with invalid references. They could be removed from the cache 
    // without deleting. Or they could be marked as invalid, and act 
    // accordingly elsewhere.
    // TODO: flag instance as invalid / non-existing?

}

void DaosPool::open() {

    if (open_) return;

    // "Cannot attempt connecting to an unidentified pool. Either create it or provide a label upon construction."
    ASSERT(known_uuid_ || label_.size() > 0);

    if (label_.size() > 0) {

        DAOS_CALL(daos_pool_connect(label_.c_str(), NULL, DAOS_PC_RW, &poh_, NULL, NULL));

    } else {

        DAOS_CALL(daos_pool_connect(uuid_, NULL, DAOS_PC_RW, &poh_, NULL, NULL));

    }
    
    open_ = true;

}

void DaosPool::close() {

    if (!open_) {
        eckit::Log::warning() << "Disconnecting DaosPool " << name() << ", pool is not open" << std::endl;
        return;
    }

    closeContainers();

    std::cout << "DAOS_CALL => daos_pool_disconnect()" << std::endl;

    int code = daos_pool_disconnect(poh_, NULL);

    if (code < 0) eckit::Log::warning() << "DAOS error in call to daos_pool_disconnect(), file " 
        << __FILE__ << ", line " << __LINE__ << ", function " << __func__ << " [" << code << "] (" 
        << strerror(-code) << ")" << std::endl;
        
    std::cout << "DAOS_CALL <= daos_pool_disconnect()" << std::endl;

    open_ = false;

}

std::deque<fdb5::DaosContainer>::iterator DaosPool::getCachedContainer(uuid_t uuid) {

    uuid_t other = {0};

    std::deque<fdb5::DaosContainer>::iterator it;
    for (it = cont_cache_.begin(); it != cont_cache_.end(); ++it) {

        it->uuid(other);
        if (uuid_compare(uuid, other) == 0) break;

    }

    return it;

}

std::deque<fdb5::DaosContainer>::iterator DaosPool::getCachedContainer(const std::string& label) {

    std::deque<fdb5::DaosContainer>::iterator it;
    for (it = cont_cache_.begin(); it != cont_cache_.end(); ++it) {

        if (it->label() == label) break;

    }

    return it;

}

fdb5::DaosContainer& DaosPool::getContainer(uuid_t uuid, bool verify) {

    std::deque<fdb5::DaosContainer>::iterator it = getCachedContainer(uuid);

    if (it != cont_cache_.end()) return *it;

    cont_cache_.push_front(fdb5::DaosContainer(*this, uuid));
    
    DaosContainer& c = cont_cache_.at(0);

    if (verify) ASSERT(c.exists());

    return c;

}

fdb5::DaosContainer& DaosPool::getContainer(const std::string& label, bool verify) {

    std::deque<fdb5::DaosContainer>::iterator it = getCachedContainer(label);

    if (it != cont_cache_.end()) return *it;
    
    cont_cache_.push_front(fdb5::DaosContainer(*this, label));
    
    DaosContainer& c = cont_cache_.at(0);

    if (verify) ASSERT(c.exists());

    return c;

}

fdb5::DaosContainer& DaosPool::getContainer(uuid_t uuid, const std::string& label, bool verify) {

    std::deque<fdb5::DaosContainer>::iterator it = getCachedContainer(uuid);
    if (it != cont_cache_.end()) {

        if (it->label() == label) return *it;

        cont_cache_.push_front(fdb5::DaosContainer(*this, uuid, label));
        return cont_cache_.at(0);

    }

    it = getCachedContainer(label);
    if (it != cont_cache_.end()) return *it;

    cont_cache_.push_front(fdb5::DaosContainer(*this, label));
    
    DaosContainer& c = cont_cache_.at(0);

    if (verify) ASSERT(c.exists());

    return c;

}

fdb5::DaosContainer& DaosPool::getContainer(uuid_t uuid) {

    return getContainer(uuid, true);

}

fdb5::DaosContainer& DaosPool::getContainer(const std::string& label) {

    return getContainer(label, true);

}

fdb5::DaosContainer& DaosPool::getContainer(uuid_t uuid, const std::string& label) {

    // When both pool uuid and label are known, using this method to declare
    // a pool is preferred to avoid the following inconsistencies and/or 
    // inefficiencies:
    // - when a user declares a pool by label in a process where that pool 
    //   has not been created, that pool will live in the cache with only a 
    //   label and no uuid. If the user later declares the same pool from its 
    //   uuid, two DaosPool instances will exist in the cache for the same 
    //   DAOS pool, each with their connection handle.
    // - these two instances will be incomplete and the user may not be able 
    //   to retrieve the label/uuid information.

    return getContainer(uuid, label, true);

}

fdb5::DaosContainer& DaosPool::createContainer(uuid_t uuid) {

    fdb5::DaosContainer& c = getContainer(uuid, false);
    
    c.create();

    return c;

}

fdb5::DaosContainer& DaosPool::createContainer(const std::string& label) {

    fdb5::DaosContainer& c = getContainer(label, false);
    
    c.create();

    return c;

}

fdb5::DaosContainer& DaosPool::createContainer(uuid_t uuid, const std::string& label) {

    fdb5::DaosContainer& c = getContainer(uuid, label, false);
    
    c.create();

    return c;

}

fdb5::DaosContainer& DaosPool::ensureContainer(uuid_t uuid) {

    fdb5::DaosContainer& c = getContainer(uuid, false);

    if (c.exists()) return c;
    
    c.create();

    return c;

}

fdb5::DaosContainer& DaosPool::ensureContainer(const std::string& label) {

    fdb5::DaosContainer& c = getContainer(label, false);
 
    if (c.exists()) return c;   

    c.create();

    return c;

}

fdb5::DaosContainer& DaosPool::ensureContainer(uuid_t uuid, const std::string& label) {

    fdb5::DaosContainer& c = getContainer(uuid, label, false);
    
    if (c.exists()) return c;

    c.create();

    return c;

}

// intended for DaosContainer::destroy()
void DaosPool::closeContainer(uuid_t uuid) {

    uuid_t other = {0};

    std::deque<fdb5::DaosContainer>::iterator it;
    for (it = cont_cache_.begin(); it != cont_cache_.end(); ++it) {

        it->uuid(other);
        if (uuid_compare(uuid, other) == 0) it->close();

    }

}

// intended for DaosContainer::destroy()
void DaosPool::closeContainer(const std::string& label) {

    uuid_t other = {0};

    std::deque<fdb5::DaosContainer>::iterator it;
    for (it = cont_cache_.begin(); it != cont_cache_.end(); ++it) {

        if (it->label() == label) it->close();

    }

}

void DaosPool::closeContainers() {

    std::deque<fdb5::DaosContainer>::iterator it;
    for (it = cont_cache_.begin(); it != cont_cache_.end(); ++it) it->close();

}

void DaosPool::destroyContainers() {

    std::deque<fdb5::DaosContainer>::iterator it;
    for (it = cont_cache_.begin(); it != cont_cache_.end(); ++it) it->destroy();

}

const daos_handle_t& DaosPool::getOpenHandle() {
    
    open();
    return poh_;

}

bool DaosPool::exists() {

    // TODO: implement this with more appropriate DAOS API functions
    try {

        open();

    } catch (eckit::SeriousBug& e) {

        return false;

    }

    return true;

}

fdb5::DaosSession& DaosPool::getSession() const {

    return session_;

}

std::string DaosPool::name() const {

    // "Cannot generate a name for an unidentified pool. Either create it or provide a label upon construction."
    ASSERT(label_.size() > 0 || known_uuid_);

    if (label_.size() > 0) return label_;

    char name_cstr[37];
    uuid_unparse(uuid_, name_cstr);
    return std::string(name_cstr);

}

void DaosPool::uuid(uuid_t uuid) const {

    uuid_copy(uuid, uuid_);

}

std::string DaosPool::label() const {

    return label_;

}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5