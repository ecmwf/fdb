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

#include <sstream>

#include "fdb5/daos/DaosSession.h"
#include "fdb5/daos/DaosException.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosSession::DaosSession(const eckit::LocalConfiguration& config) : 
    containerOidsPerAlloc_(DaosManager::instance().containerOidsPerAlloc()),
    objectCreateCellSize_(DaosManager::instance().objectCreateCellSize()),
    objectCreateChunkSize_(DaosManager::instance().objectCreateChunkSize()) {

    containerOidsPerAlloc_ = config.getInt("container_oids_per_alloc", containerOidsPerAlloc_);
    objectCreateCellSize_ = config.getInt64("object_create_cell_size", objectCreateCellSize_);
    objectCreateChunkSize_ = config.getInt64("object_create_chunk_size", objectCreateChunkSize_);

    // daos_init can be called multiple times. An internal reference count is maintained by the library
    DAOS_CALL(daos_init());

}

DaosSession::~DaosSession() {

    pool_cache_.clear();

    std::cout << "DAOS_CALL => daos_fini()" << std::endl;

    int code = daos_fini();

    if (code < 0) eckit::Log::warning() << "DAOS error in call to daos_fini(), file " 
        << __FILE__ << ", line " << __LINE__ << ", function " << __func__ << " [" << code << "] (" 
        << strerror(-code) << ")" << std::endl;

    std::cout << "DAOS_CALL <= daos_fini()" << std::endl;

}

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

fdb5::DaosPool& DaosSession::getPool(uuid_t uuid) {

    std::deque<fdb5::DaosPool>::iterator it = getCachedPool(uuid);

    if (it != pool_cache_.end()) return *it;

    fdb5::DaosPool p(*this, uuid);

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

    fdb5::DaosPool p(*this, label);

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

        pool_cache_.push_front(fdb5::DaosPool(*this, uuid, label));
        return pool_cache_.at(0);

    }

    it = getCachedPool(label);
    if (it != pool_cache_.end()) return *it;

    fdb5::DaosPool p(*this, uuid, label);

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
fdb5::DaosPool& DaosSession::createPool(const uint64_t& scmSize, const uint64_t& nvmeSize) {

    pool_cache_.push_front(fdb5::DaosPool(*this));

    fdb5::DaosPool& p = pool_cache_.at(0);

    p.create(scmSize, nvmeSize);

    return p;

}

fdb5::DaosPool& DaosSession::createPool(const std::string& label, const uint64_t& scmSize, const uint64_t& nvmeSize) {

    pool_cache_.push_front(fdb5::DaosPool(*this, label));

    fdb5::DaosPool& p = pool_cache_.at(0);
    
    p.create(scmSize, nvmeSize);

    return p;

}

// intended for DaosPool::destroy(), where all potentially cached pools with 
// a given uuid need to be closed
void DaosSession::closePool(uuid_t uuid) {

    uuid_t other = {0};

    std::deque<fdb5::DaosPool>::iterator it;
    for (it = pool_cache_.begin(); it != pool_cache_.end(); ++it) {

        it->uuid(other);
        if (uuid_compare(uuid, other) == 0) it->close();

    }

}
#endif

void DaosSession::destroyPoolContainers(uuid_t uuid) {

    uuid_t other = {0};

    std::deque<fdb5::DaosPool>::iterator it;
    for (it = pool_cache_.begin(); it != pool_cache_.end(); ++it) {

        it->uuid(other);
        if (uuid_compare(uuid, other) == 0) it->destroyContainers();

    }

}

void DaosSession::error(int code, const char* msg, const char* file, int line, const char* func) {

    std::ostringstream oss;
    oss << "DAOS error " << msg << ", file " << file << ", line " << line << ", function " << func << " [" << code
        << "] (" << strerror(-code) << ")";
    throw eckit::SeriousBug(oss.str());
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5