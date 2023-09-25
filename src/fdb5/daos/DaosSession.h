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
/// @date Jul 2022

#pragma once

#include <daos.h>

#include <string>
#include <deque>
#include <mutex>

#include "eckit/exception/Exceptions.h"
#include "eckit/config/LocalConfiguration.h"
#include "eckit/config/Resource.h"

#include "fdb5/daos/DaosPool.h"
#include "fdb5/daos/DaosException.h"

#include "fdb5/fdb5_config.h"

namespace fdb5 {

/// @todo: Use std::map<std::string, fdb5::DaosPool>
/// @todo: Offload caching to manager?
using PoolCache = std::deque<fdb5::DaosPool>;

class DaosManager : private eckit::NonCopyable {

public: // methods

    /// @todo: set configuration where relvant in unit tests
    /// @todo: unit tests for config
    static DaosManager& instance() {
        static DaosManager instance;
        return instance;
    };

    static void error(int code, const char* msg, const char* file, int line, const char* func);

    void configure(const eckit::LocalConfiguration&);

    // int containerOidsPerAlloc() const { return containerOidsPerAlloc_; };
    // uint64_t objectCreateCellSize() const { return objectCreateCellSize_; };
    // uint64_t objectCreateChunkSize() const { return objectCreateChunkSize_; };
    // std::string dmgConfigFile() const { return dmgConfigFile_; };

private: // methods

    DaosManager();

    /// @todo: should configure here with LibFdb5 default config, for cases where DaosManager is never configured?
    //       I would say No. fdb5 classes should always configure Daos including configuration from LibFdb5 default config.
    //       Daos* classes should not depend on fdb5 configuration if used independently.

    ~DaosManager();

private: // members

    friend class DaosSession;

    std::recursive_mutex mutex_;
    PoolCache pool_cache_;

    int containerOidsPerAlloc_;
    uint64_t objectCreateCellSize_;
    uint64_t objectCreateChunkSize_;
    std::string dmgConfigFile_;

};

#define DAOS_CALL(a) fdb5::daos_call(a, #a, __FILE__, __LINE__, __func__)

static inline int daos_call(int code, const char* msg, const char* file, int line, const char* func) {

    // std::cout << "DAOS_CALL => " << msg << std::endl;

    if (code < 0) {
        // std::cout << "DAOS_FAIL !! " << msg << std::endl;
        if (code == -DER_NONEXIST) throw fdb5::DaosEntityNotFoundException(msg);
        if (code == -DER_EXIST) throw fdb5::DaosEntityAlreadyExistsException(msg);
        DaosManager::error(code, msg, file, line, func);
    }

    // std::cout << "DAOS_CALL <= " << msg << std::endl;

    return code;
}

//----------------------------------------------------------------------------------------------------------------------

/// @note: DaosSession acts as a mere wrapper for DaosManager such that DaosManager::instance does not need
///   to be called in many places
/// @note: DaosSession no longer performs daos_init on creation and daos_fini on destroy. This is because 
///   any pool handles obtained within a session are cached in DaosManager beyond DaosSession lifetime, 
///   and the pool handles may become invalid if daos_fini is called for all sessions

class DaosSession : eckit::NonCopyable {

public: // methods

    DaosSession();
    ~DaosSession() {};

#ifdef fdb5_HAVE_DAOS_ADMIN
    // administrative
    fdb5::DaosPool& createPool(
        const uint64_t& scmSize = 10ULL << 30, 
        const uint64_t& nvmeSize = 40ULL << 30);
    fdb5::DaosPool& createPool(
        const std::string& label, 
        const uint64_t& scmSize = 10ULL << 30, 
        const uint64_t& nvmeSize = 40ULL << 30);
    void destroyPool(uuid_t, const int& force = 1);
#endif

    fdb5::DaosPool& getPool(uuid_t);
    fdb5::DaosPool& getPool(const std::string&);
    fdb5::DaosPool& getPool(uuid_t, const std::string&);

    int containerOidsPerAlloc() const { return DaosManager::instance().containerOidsPerAlloc_; };
    uint64_t objectCreateCellSize() const { return DaosManager::instance().objectCreateCellSize_; };
    uint64_t objectCreateChunkSize() const { return DaosManager::instance().objectCreateChunkSize_; };
    std::string dmgConfigFile() const { return DaosManager::instance().dmgConfigFile_; };

private: // methods

    PoolCache::iterator getCachedPool(uuid_t);
    PoolCache::iterator getCachedPool(const std::string&);

// #ifdef fdb5_HAVE_DAOS_ADMIN
    // void closePool(uuid_t);
    // void destroyPoolContainers(uuid_t);
// #endif

private: // members

    /// @todo: add lock_guards in all DaosSession methods using pool_cache_. Same for cont_cache_ in DaosPool.
    // std::recursive_mutex& mutex_;
    PoolCache& pool_cache_;

};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
