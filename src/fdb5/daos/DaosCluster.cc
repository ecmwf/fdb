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

#include "fdb5/daos/DaosCluster.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

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

std::deque<fdb5::DaosPool>::iterator DaosCluster::getCachedPool(uuid_t uuid) {

    uuid_t other = {0};

    std::deque<fdb5::DaosPool>::iterator it;
    for (it = pool_cache_.begin(); it != pool_cache_.end(); ++it) {

        it->uuid(other);
        if (uuid_compare(uuid, other) == 0) break;

    }

    return it;

}

std::deque<fdb5::DaosPool>::iterator DaosCluster::getCachedPool(const std::string& label) {

    std::deque<fdb5::DaosPool>::iterator it;
    for (it = pool_cache_.begin(); it != pool_cache_.end(); ++it) {

        if (it->label() == label) break;

    }

    return it;

}

fdb5::DaosPool& DaosCluster::declarePool(uuid_t uuid) {

    std::deque<fdb5::DaosPool>::iterator it = getCachedPool(uuid);

    if (it != pool_cache_.end()) return *it;

    pool_cache_.push_front(fdb5::DaosPool(uuid));
    
    return pool_cache_.at(0);

}

fdb5::DaosPool& DaosCluster::declarePool(const std::string& label) {

    std::deque<fdb5::DaosPool>::iterator it = getCachedPool(label);

    if (it != pool_cache_.end()) return *it;
    
    pool_cache_.push_front(fdb5::DaosPool(label));
    
    return pool_cache_.at(0);

}

DaosPool& DaosCluster::declarePool(uuid_t uuid, const std::string& label) {

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

    pool_cache_.push_front(fdb5::DaosPool(label));
    return pool_cache_.at(0);

}

fdb5::DaosPool& DaosCluster::createPool() {

    pool_cache_.push_front(fdb5::DaosPool());

    fdb5::DaosPool& p = pool_cache_.at(0);

    p.create();

    return p;

}

fdb5::DaosPool& DaosCluster::createPool(const std::string& label) {

    fdb5::DaosPool& p = declarePool(label);
    
    p.create();

    return p;

}

void DaosCluster::destroyPool(uuid_t uuid) {

    // TODO: make getCachedPool return a *DaosPool rather than an iterator?
    // and check it == nullptr rather than it == pool_cache_.end().
    std::deque<fdb5::DaosPool>::iterator it = getCachedPool(uuid);

    if (it == pool_cache_.end()) {
        char uuid_cstr[37];
        uuid_unparse(uuid, uuid_cstr);
        std::string uuid_str(uuid_cstr);
        throw eckit::Exception("Pool with uuid " + uuid_str + " not found in cache, cannot destroy.");
    }

    it->destroy();

}

void DaosCluster::closePool(uuid_t uuid) {

    uuid_t other = {0};

    std::deque<fdb5::DaosPool>::iterator it;
    for (it = pool_cache_.begin(); it != pool_cache_.end(); ++it) {

        it->uuid(other);
        if (uuid_compare(uuid, other) == 0) it->close();

    }

}

void DaosCluster::error(int code, const char* msg, const char* file, int line, const char* func) {

    std::ostringstream oss;
    oss << "DAOS error " << msg << ", file " << file << ", line " << line << ", function " << func << " [" << code
        << "] (" << strerror(-code) << ")";
    throw eckit::SeriousBug(oss.str());
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5