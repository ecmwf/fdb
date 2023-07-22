/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/exception/Exceptions.h"

#include "fdb5/daos/DaosPool.h"
#include "fdb5/daos/DaosSession.h"
#include "fdb5/daos/DaosException.h"

#ifdef fdb5_HAVE_DAOS_ADMIN
extern "C" {
#include <daos/tests_lib.h>
}
#endif

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosPool::DaosPool(DaosPool&& other) noexcept : known_uuid_(other.known_uuid_), 
    label_(std::move(other.label_)), poh_(std::move(other.poh_)), open_(other.open_),
    cont_cache_(std::move(other.cont_cache_)) {

    uuid_copy(uuid_, other.uuid_);
    other.open_ = false;

}

DaosPool::DaosPool() : known_uuid_(false), open_(false) {}

DaosPool::DaosPool(uuid_t uuid) : known_uuid_(true), open_(false) {

    uuid_copy(uuid_, uuid);

}

DaosPool::DaosPool(const std::string& label) : known_uuid_(false), label_(label), open_(false) {}


DaosPool::DaosPool(uuid_t uuid, const std::string& label) : known_uuid_(true), label_(label), open_(false) {

    uuid_copy(uuid_, uuid);

}

DaosPool::~DaosPool() {

    cont_cache_.clear();

    if (open_) close();

}

DaosPool& DaosPool::operator=(DaosPool&& other) noexcept {

    known_uuid_ = other.known_uuid_;
    label_ = std::move(other.label_);
    poh_ = std::move(other.poh_);
    open_ = other.open_;
    cont_cache_ = std::move(other.cont_cache_);

    uuid_copy(uuid_, other.uuid_);
    other.open_ = false;

    return *this;

}

#ifdef fdb5_HAVE_DAOS_ADMIN
void DaosPool::create(const uint64_t& scmSize, const uint64_t& nvmeSize) {

    // "Cannot create a pool with a user-specified UUID."
    ASSERT(!known_uuid_);

    /// @todo: ensure deallocation. Either try catch or make a wrapper.
    /// @todo: rename where possible to make less obscure
    d_rank_list_t svcl;
    svcl.rl_nr = 1;
    D_ALLOC_ARRAY(svcl.rl_ranks, svcl.rl_nr);
    ASSERT(svcl.rl_ranks);

    daos_prop_t *prop = NULL;

    if (label_.size() > 0) {

        /// @todo: Ensure freeing. default_delete.
        // Throw exception if that fails.
        prop = daos_prop_alloc(1);
        prop->dpp_entries[0].dpe_type = DAOS_PROP_PO_LABEL;
        D_STRNDUP(prop->dpp_entries[0].dpe_str, label_.c_str(), DAOS_PROP_LABEL_MAX_LEN);      

    }

    DAOS_CALL(
        dmg_pool_create(
            fdb5::DaosSession().dmgConfigFile().c_str(), geteuid(), getegid(), NULL, NULL, 
            scmSize, nvmeSize, 
            prop, &svcl, uuid_
        )
    );

    /// @todo: query the pool to ensure it's ready

    if (prop != NULL) daos_prop_free(prop);

    D_FREE(svcl.rl_ranks);

    known_uuid_ = true;

}
#endif

void DaosPool::open() {

    if (open_) return;

    ASSERT(known_uuid_ || label_.size() > 0);
    
    if (label_.size() > 0) {
        DAOS_CALL(daos_pool_connect(label_.c_str(), NULL, DAOS_PC_RW, &poh_, NULL, NULL));
    } else {
        char uuid_cstr[37] = "";
        uuid_unparse(uuid_, uuid_cstr);
        DAOS_CALL(daos_pool_connect(uuid_cstr, NULL, DAOS_PC_RW, &poh_, NULL, NULL));
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
        << code << ")" << std::endl;
        
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

    fdb5::DaosContainer c(*this, uuid);

    if (verify && !c.exists()) {
        char uuid_cstr[37];
        uuid_unparse(uuid, uuid_cstr);
        std::string uuid_str(uuid_cstr);
        throw fdb5::DaosEntityNotFoundException(
            "Container with uuid " + uuid_str + " not found", 
            Here());
    }
    
    cont_cache_.push_front(std::move(c));
    
    return cont_cache_.at(0);

}

fdb5::DaosContainer& DaosPool::getContainer(const std::string& label, bool verify) {

    std::deque<fdb5::DaosContainer>::iterator it = getCachedContainer(label);

    if (it != cont_cache_.end()) return *it;
    
    fdb5::DaosContainer c(*this, label);

    if (verify && !c.exists()) {
        throw fdb5::DaosEntityNotFoundException(
            "Container with label " + label + " not found", 
            Here());
    }

    cont_cache_.push_front(std::move(c));
    
    return cont_cache_.at(0);

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

    fdb5::DaosContainer c(*this, label);

    if (verify && !c.exists()) {
        char uuid_cstr[37];
        uuid_unparse(uuid, uuid_cstr);
        std::string uuid_str(uuid_cstr);
        throw fdb5::DaosEntityNotFoundException(
            "Container with uuid " + uuid_str + " or label " + label + " not found", 
            Here());
    }

    cont_cache_.push_front(std::move(c));
    
    return cont_cache_.at(0);

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

fdb5::DaosContainer& DaosPool::createContainer() {

    fdb5::DaosContainer c(*this);

    cont_cache_.push_front(std::move(c));

    cont_cache_.at(0).create();
    
    return cont_cache_.at(0);

}

fdb5::DaosContainer& DaosPool::createContainer(const std::string& label) {

    fdb5::DaosContainer& c = getContainer(label, false);

    /// @todo: if the container is found in the cache, it can be assumed
    ///   it exists and this creation could be skipped
    c.create();

    return c;

}

fdb5::DaosContainer& DaosPool::ensureContainer(const std::string& label) {

    fdb5::DaosContainer& c = getContainer(label, false);

    /// @todo: if the container is found in the cache, it can be assumed
    ///   it exists and this check could be skipped
    if (c.exists()) return c;   

    c.create();

    return c;

}

void DaosPool::destroyContainer(const std::string& label) {

    ASSERT(label.size() > 0);

    /// @todo: consider cases where DaosContainer instances may be cached
    ///   which only have a uuid and no label, but correspond to the same 
    ///   container being destroyed here

    bool found = false;

    std::deque<fdb5::DaosContainer>::iterator it;
    for (it = cont_cache_.begin(); it != cont_cache_.end(); ++it) {

        if (label == it->label()) {
            found = true;
            cont_cache_.erase(it);
        }

    }

    if (!found) {

        throw fdb5::DaosEntityNotFoundException(
            "Container with label " + label + " not found",
            Here());

    }

    DAOS_CALL(daos_cont_destroy(poh_, label.c_str(), 1, NULL));

    /// @todo: whenever a container is destroyed and the user owns open objects, their handles may not
    ///   be possible to close anymore, and any actions on such objects will fail

}

// indended for DaosPool::close()
void DaosPool::closeContainers() {

    std::deque<fdb5::DaosContainer>::iterator it;
    for (it = cont_cache_.begin(); it != cont_cache_.end(); ++it) it->close();

}

// void DaosPool::destroyContainers() {
// 
//     std::deque<fdb5::DaosContainer>::iterator it;
//     for (it = cont_cache_.begin(); it != cont_cache_.end(); ++it) it->destroy();
//     if reenabled, this code needs to be modified to call pool.destroyContainer(uuid or label)
// 
// }

std::vector<std::string> DaosPool::listContainers() {

    std::vector<std::string> res;

    daos_size_t ncont{0};
    DAOS_CALL(daos_pool_list_cont(getOpenHandle(), &ncont, NULL, NULL));

    if (ncont == 0) return res;

    std::vector<struct daos_pool_cont_info> info(ncont);
    DAOS_CALL(daos_pool_list_cont(getOpenHandle(), &ncont, info.data(), NULL));

    for (const auto& c : info) res.push_back(c.pci_label);

    return res;

}

const daos_handle_t& DaosPool::getOpenHandle() {
    
    open();
    return poh_;

}

bool DaosPool::exists() {

    /// @todo: implement this with more appropriate DAOS API functions
    try {

        open();

    } catch (eckit::SeriousBug& e) {

        return false;

    }

    return true;

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

AutoPoolDestroy::~AutoPoolDestroy() noexcept(false) {

    bool fail = !eckit::Exception::throwing();

    try {
        uuid_t u{0};
        pool_.uuid(u);
        fdb5::DaosSession().destroyPool(u);
    }
    catch (std::exception& e) {
        eckit::Log::error() << "** " << e.what() << " Caught in " << Here() << std::endl;
        if (fail) {
            eckit::Log::error() << "** Exception is re-thrown" << std::endl;
            throw;
        }
        eckit::Log::error() << "** An exception is already in progress" << std::endl;
        eckit::Log::error() << "** Exception is ignored" << std::endl;
    }
    
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
