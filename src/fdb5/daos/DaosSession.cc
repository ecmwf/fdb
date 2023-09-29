/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <sstream>

#include "eckit/runtime/Main.h"
#include "eckit/utils/Translator.h"

#include "fdb5/daos/DaosSession.h"
#include "fdb5/daos/DaosException.h"

#ifdef fdb5_HAVE_DAOS_ADMIN
extern "C" {
#include <daos/tests_lib.h>
}
#endif

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosManager::DaosManager() : 
    containerOidsPerAlloc_(100),
    objectCreateCellSize_(1),
    objectCreateChunkSize_(1048576),
    stats_(std::string("FDB DAOS profiling ") + eckit::Main::hostname() + ":" + eckit::Translator<int, std::string>()(::getpid())) {

    dmgConfigFile_ = eckit::Resource<std::string>(
        "fdbDaosDmgConfigFile;$FDB_DAOS_DMG_CONFIG_FILE", dmgConfigFile_
    );

    // daos_init can be called multiple times. An internal reference count is maintained by the library
    DAOS_CALL(daos_init());

}

DaosManager::~DaosManager() {

    pool_cache_.clear();

    // std::cout << "DAOS_CALL => daos_fini()" << std::endl;

    int code = daos_fini();

    if (code < 0) eckit::Log::warning() << "DAOS error in call to daos_fini(), file " 
        << __FILE__ << ", line " << __LINE__ << ", function " << __func__ << " [" << code << "] (" 
        << code << ")" << std::endl;

    // std::cout << "DAOS_CALL <= daos_fini()" << std::endl;

}

void DaosManager::error(int code, const char* msg, const char* file, int line, const char* func) {

    std::ostringstream oss;
    oss << "DAOS error " << msg << ", file " << file << ", line " << line << ", function " << func << " [" << code
        << "] (" << code << ")";
    throw eckit::SeriousBug(oss.str());

}

void DaosManager::configure(const eckit::LocalConfiguration& config) {

    std::lock_guard<std::recursive_mutex> lock(mutex_);
    containerOidsPerAlloc_ = config.getInt("container_oids_per_alloc", containerOidsPerAlloc_);
    objectCreateCellSize_ = config.getInt64("object_create_cell_size", objectCreateCellSize_);
    objectCreateChunkSize_ = config.getInt64("object_create_chunk_size", objectCreateChunkSize_);
    dmgConfigFile_ = config.getString("dmg_config_file", dmgConfigFile_);

};

//----------------------------------------------------------------------------------------------------------------------

DaosSession::DaosSession() : 
    // mutex_(DaosManager::instance().mutex_),
    pool_cache_(DaosManager::instance().pool_cache_) {}

// DaosSession::~DaosSession() {}

/// @todo: make getCachedPool return a *DaosPool rather than an iterator?
// and check it == nullptr rather than it == pool_cache_.end().
std::deque<fdb5::DaosPool>::iterator DaosSession::getCachedPool(uuid_t uuid) {

    uuid_t other = {0};

    std::deque<fdb5::DaosPool>::iterator it;
    for (it = pool_cache_.begin(); it != pool_cache_.end(); ++it) {

        it->uuid(other);
        if (uuid_compare(uuid, other) == 0) break;

    }

    return it;

}

std::deque<fdb5::DaosPool>::iterator DaosSession::getCachedPool(const std::string& label) {

    std::deque<fdb5::DaosPool>::iterator it;
    for (it = pool_cache_.begin(); it != pool_cache_.end(); ++it) {

        if (it->label() == label) break;

    }

    return it;

}

#ifdef fdb5_HAVE_DAOS_ADMIN
fdb5::DaosPool& DaosSession::createPool(const uint64_t& scmSize, const uint64_t& nvmeSize) {

    pool_cache_.push_front(fdb5::DaosPool());

    fdb5::DaosPool& p = pool_cache_.at(0);

    p.create(scmSize, nvmeSize);

    return p;

}

fdb5::DaosPool& DaosSession::createPool(const std::string& label, const uint64_t& scmSize, const uint64_t& nvmeSize) {

    pool_cache_.push_front(fdb5::DaosPool(label));
    
    fdb5::DaosPool& p = pool_cache_.at(0);
    
    p.create(scmSize, nvmeSize);

    return p;

}
#endif

fdb5::DaosPool& DaosSession::getPool(uuid_t uuid) {

    std::deque<fdb5::DaosPool>::iterator it = getCachedPool(uuid);

    if (it != pool_cache_.end()) return *it;

    fdb5::DaosPool p(uuid);

    if (!p.exists()) {
        char uuid_cstr[37];
        uuid_unparse(uuid, uuid_cstr);
        std::string uuid_str(uuid_cstr);
        throw fdb5::DaosEntityNotFoundException(
            "Pool with uuid " + uuid_str + " not found", 
            Here());
    }

    pool_cache_.push_front(std::move(p));

    return pool_cache_.at(0);

}

fdb5::DaosPool& DaosSession::getPool(const std::string& label) {

    std::deque<fdb5::DaosPool>::iterator it = getCachedPool(label);

    if (it != pool_cache_.end()) return *it;

    fdb5::DaosPool p(label);

    if (!p.exists()) {
        throw fdb5::DaosEntityNotFoundException(
            "Pool with label " + label + " not found", 
            Here());
    }

    pool_cache_.push_front(std::move(p));

    return pool_cache_.at(0);

}

DaosPool& DaosSession::getPool(uuid_t uuid, const std::string& label) {

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

    std::deque<fdb5::DaosPool>::iterator it = getCachedPool(uuid);
    if (it != pool_cache_.end()) {

        if (it->label() == label) return *it;

        pool_cache_.push_front(fdb5::DaosPool(uuid, label));
        return pool_cache_.at(0);

    }

    it = getCachedPool(label);
    if (it != pool_cache_.end()) return *it;

    fdb5::DaosPool p(uuid, label);

    if (!p.exists()) {
        char uuid_cstr[37];
        uuid_unparse(uuid, uuid_cstr);
        std::string uuid_str(uuid_cstr);
        throw fdb5::DaosEntityNotFoundException(
            "Pool with uuid " + uuid_str + " or label " + label + " not found", 
            Here());
    }

    pool_cache_.push_front(std::move(p));

    return pool_cache_.at(0);

}

#ifdef fdb5_HAVE_DAOS_ADMIN
// intended for destroyPool(), where all potentially cached pools with 
// a given uuid need to be closed
// void DaosSession::closePool(uuid_t uuid) {
// 
//     uuid_t other = {0};
// 
//     std::deque<fdb5::DaosPool>::iterator it;
//     for (it = pool_cache_.begin(); it != pool_cache_.end(); ++it) {
// 
//         it->uuid(other);
//         if (uuid_compare(uuid, other) == 0) it->close();
// 
//     }
// 
// }
// 
// void DaosSession::destroyPoolContainers(uuid_t uuid) {
// 
//     uuid_t other = {0};
// 
//     std::deque<fdb5::DaosPool>::iterator it;
//     for (it = pool_cache_.begin(); it != pool_cache_.end(); ++it) {
// 
//         it->uuid(other);
//         if (uuid_compare(uuid, other) == 0) it->destroyContainers();
// 
//     }
// 
// }

void DaosSession::destroyPool(uuid_t uuid, const int& force) {

    bool found = false;
    uuid_t other = {0};
    std::string label = std::string();

    std::deque<fdb5::DaosPool>::iterator it = pool_cache_.begin();
    while (it != pool_cache_.end()) {

        it->uuid(other);
        if (uuid_compare(uuid, other) == 0) {
            found = true;
            label = it->label();
            /// @todo: should destroy pool containers here?
            it = pool_cache_.erase(it);
        } else {
            ++it;
        }

    }

    if (!found) {

        char uuid_cstr[37];
        uuid_unparse(uuid, uuid_cstr);
        std::string uuid_str(uuid_cstr);
        throw fdb5::DaosEntityNotFoundException(
            "Pool with uuid " + uuid_str + " not found", 
            Here());

    }

    DAOS_CALL(dmg_pool_destroy(dmgConfigFile().c_str(), uuid, NULL, force));

    /// @todo: cached DaosPools declared with a label only, pointing to the pool
    // being destroyed may still exist and should be closed and removed

    /// @note: creating a pool with the same label immediately after destroy
    ///        sometimes fails.
    // sleep(1);

}
#endif

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
