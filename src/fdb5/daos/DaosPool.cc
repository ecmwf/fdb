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

#include "fdb5/LibFdb5.h"
#include "fdb5/daos/DaosException.h"
#include "fdb5/daos/DaosPool.h"
#include "fdb5/daos/DaosSession.h"

#ifdef fdb5_HAVE_DAOS_ADMIN
extern "C" {
#include <daos/tests_lib.h>
}
#endif

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosPool::DaosPool(DaosPool&& other) noexcept :
    uuid_(std::move(other.uuid_)),
    known_uuid_(other.known_uuid_),
    label_(std::move(other.label_)),
    poh_(std::move(other.poh_)),
    open_(other.open_),
    cont_cache_(std::move(other.cont_cache_)) {

    other.open_ = false;
}

DaosPool::DaosPool() : known_uuid_(false), open_(false) {}

DaosPool::DaosPool(const fdb5::UUID& uuid) : uuid_(uuid), known_uuid_(true), open_(false) {}

DaosPool::DaosPool(const std::string& label) : known_uuid_(false), label_(label), open_(false) {}


DaosPool::DaosPool(const fdb5::UUID& uuid, const std::string& label) :
    uuid_(uuid), known_uuid_(true), label_(label), open_(false) {}

DaosPool::~DaosPool() {

    cont_cache_.clear();

    if (open_) {
        close();
    }
}

DaosPool& DaosPool::operator=(DaosPool&& other) noexcept {

    uuid_ = std::move(other.uuid_);
    known_uuid_ = other.known_uuid_;
    label_ = std::move(other.label_);
    poh_ = std::move(other.poh_);
    open_ = other.open_;
    cont_cache_ = std::move(other.cont_cache_);

    other.open_ = false;

    return *this;
}

#ifdef fdb5_HAVE_DAOS_ADMIN
void DaosPool::create(const uint64_t& scmSize, const uint64_t& nvmeSize) {

    /// @note: cannot create a pool with a user-specified UUID
    ASSERT(!known_uuid_);

    /// @todo: ensure deallocation. Either try catch or make a wrapper.
    /// @todo: rename where possible to make less obscure
    d_rank_list_t svcl;
    svcl.rl_nr = 1;
    D_ALLOC_ARRAY(svcl.rl_ranks, svcl.rl_nr);
    ASSERT(svcl.rl_ranks);

    daos_prop_t* prop = NULL;

    if (label_.size() > 0) {

        /// @todo: Ensure freeing. default_delete.
        // Throw exception if that fails.
        prop = daos_prop_alloc(1);
        prop->dpp_entries[0].dpe_type = DAOS_PROP_PO_LABEL;
        D_STRNDUP(prop->dpp_entries[0].dpe_str, label_.c_str(), DAOS_PROP_LABEL_MAX_LEN);
    }

    DAOS_CALL(dmg_pool_create(fdb5::DaosSession().dmgConfigFile().c_str(), geteuid(), getegid(), NULL, NULL, scmSize,
                              nvmeSize, prop, &svcl, uuid_.internal));

    /// @todo: query the pool to ensure it's ready

    if (prop != NULL) {
        daos_prop_free(prop);
    }

    D_FREE(svcl.rl_ranks);

    known_uuid_ = true;
}
#endif

void DaosPool::open() {

    if (open_) {
        return;
    }

    ASSERT(known_uuid_ || label_.size() > 0);

    if (label_.size() > 0) {
        DAOS_CALL(daos_pool_connect(label_.c_str(), NULL, DAOS_PC_RW, &poh_, NULL, NULL));
    }
    else {
        char uuid_cstr[37] = "";
        uuid_unparse(uuid_.internal, uuid_cstr);
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

    LOG_DEBUG_LIB(LibFdb5) << "DAOS_CALL => daos_pool_disconnect()" << std::endl;

    int code = daos_pool_disconnect(poh_, NULL);

    if (code < 0) {
        eckit::Log::warning() << "DAOS error in call to daos_pool_disconnect(), file " << __FILE__ << ", line "
                              << __LINE__ << ", function " << __func__ << " [" << code << "] (" << code << ")"
                              << std::endl;
    }

    LOG_DEBUG_LIB(LibFdb5) << "DAOS_CALL <= daos_pool_disconnect()" << std::endl;

    open_ = false;
}

std::map<std::string, fdb5::DaosContainer>::iterator DaosPool::getCachedContainer(const std::string& label) {

    return cont_cache_.find(label);
}

fdb5::DaosContainer& DaosPool::getContainer(const std::string& label, bool verify) {

    std::map<std::string, fdb5::DaosContainer>::iterator it = getCachedContainer(label);

    if (it != cont_cache_.end()) {
        return it->second;
    }

    fdb5::DaosContainer c{*this, label};

    if (verify && !c.exists()) {
        throw fdb5::DaosEntityNotFoundException("Container with label " + label + " not found", Here());
    }

    return cont_cache_.emplace(label, std::move(c)).first->second;
}

fdb5::DaosContainer& DaosPool::getContainer(const std::string& label) {

    return getContainer(label, true);
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
    if (c.exists()) {
        return c;
    }

    c.create();

    return c;
}

void DaosPool::destroyContainer(const std::string& label) {

    ASSERT(label.size() > 0);

    /// @todo: consider cases where DaosContainer instances may be cached
    ///   which only have a uuid and no label, but correspond to the same
    ///   container being destroyed here

    bool found = false;

    std::map<std::string, fdb5::DaosContainer>::iterator it = getCachedContainer(label);

    if (it != cont_cache_.end()) {

        cont_cache_.erase(it);
    }
    else {

        throw fdb5::DaosEntityNotFoundException("Container with label " + label + " not found", Here());
    }

    DAOS_CALL(daos_cont_destroy(poh_, label.c_str(), 1, NULL));

    /// @todo: whenever a container is destroyed and the user owns open objects, their handles may not
    ///   be possible to close anymore, and any actions on such objects will fail
}

/// @note: indended for DaosPool::close()
void DaosPool::closeContainers() {

    std::map<std::string, fdb5::DaosContainer>::iterator it;
    for (it = cont_cache_.begin(); it != cont_cache_.end(); ++it) {
        it->second.close();
    }
}

std::vector<std::string> DaosPool::listContainers() {

    std::vector<std::string> res;

    daos_size_t ncont{0};
    DAOS_CALL(daos_pool_list_cont(getOpenHandle(), &ncont, NULL, NULL));

    if (ncont == 0) {
        return res;
    }

    std::vector<struct daos_pool_cont_info> info(ncont);
    DAOS_CALL(daos_pool_list_cont(getOpenHandle(), &ncont, info.data(), NULL));

    for (const auto& c : info) {
        res.push_back(c.pci_label);
    }

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
    }
    catch (eckit::SeriousBug& e) {

        return false;
    }

    return true;
}

std::string DaosPool::name() const {

    /// @note: cannot generate a name for an unidentified pool.
    ///   Either create it or provide a label upon construction.
    ASSERT(label_.size() > 0 || known_uuid_);

    if (label_.size() > 0) {
        return label_;
    }

    char name_cstr[37];
    uuid_unparse(uuid_.internal, name_cstr);
    return std::string(name_cstr);
}

const fdb5::UUID& DaosPool::uuid() const {

    return uuid_;
}

std::string DaosPool::label() const {

    return label_;
}

//----------------------------------------------------------------------------------------------------------------------

#ifdef fdb5_HAVE_DAOS_ADMIN
AutoPoolDestroy::~AutoPoolDestroy() noexcept(false) {

    bool fail = !eckit::Exception::throwing();

    try {
        fdb5::DaosSession().destroyPool(pool_.uuid());
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
#endif

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
