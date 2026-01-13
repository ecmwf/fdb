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

#include <deque>
#include <mutex>
#include <string>

#include "eckit/config/LocalConfiguration.h"
#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/log/Timer.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/daos/DaosException.h"
#include "fdb5/daos/DaosPool.h"

#include "fdb5/fdb5_config.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// @todo: Use std::map<std::string, fdb5::DaosPool>
/// @todo: Offload caching to manager?
using PoolCache = std::deque<fdb5::DaosPool>;

/// @todo: move to a separate file
class DaosManager {

public:  // methods

    static DaosManager& instance() {
        static DaosManager instance;
        return instance;
    };

    DaosManager(const DaosManager&)            = delete;
    DaosManager& operator=(const DaosManager&) = delete;
    DaosManager(DaosManager&&)                 = delete;
    DaosManager& operator=(DaosManager&&)      = delete;

    static void error(int code, const char* msg, const char* file, int line, const char* func);

    void configure(const eckit::LocalConfiguration&);

private:  // methods

    DaosManager();

    ~DaosManager();

private:  // members

    friend class DaosSession;

    std::mutex mutex_;
    PoolCache pool_cache_;

    /// @note: sets number of OIDs allocated in a single daos_cont_alloc_oids call
    int containerOidsPerAlloc_;
    /// @note: number of bytes per cell in a DAOS object
    /// @note: cell size is the unit of atomicity / update. If the object
    ///   will always be updated or read in elements of e.g. 64k, then 64k
    ///   can be used as the cell size. Then, that 64k element cannot be
    ///   partially updated anymore.
    uint64_t objectCreateCellSize_;
    /// @note: number of cells of per dkey in a DAOS object
    /// @note: the chunk size maps to how many cells to put under 1 dkey.
    ///   So it also controls the RPC size. It should not really be something
    ///   very small otherwise it might create a lot of RPCs. If not
    ///   using redundancy (SX), setting it to something as equal or a multiple
    ///   of the most common transfer size is OK. the default is 1 MiB which is
    ///   usually OK. If using EC, it gets more tricky as the EC cell size and
    ///   transfer size come into play and break that even more and can cause
    ///   overhead on the client side. Ideally, set both to the same as the IO
    ///   size, but that is not always possible because the EC cell size is
    ///   only changed per container, vs the chunk and transfer size that can
    ///   vary per object / file. That may be changed in DAOS to allow more
    ///   flexibility
    uint64_t objectCreateChunkSize_;

#ifdef fdb5_HAVE_DAOS_ADMIN
    std::string dmgConfigFile_;
#endif
};

#define DAOS_CALL(a) fdb5::daos_call(a, #a, __FILE__, __LINE__, __func__)

static inline int daos_call(int code, const char* msg, const char* file, int line, const char* func) {

    LOG_DEBUG_LIB(LibFdb5) << "DAOS_CALL => " << msg << std::endl;

    if (code < 0) {
        eckit::Log::error() << "DAOS_FAIL !! " << msg << std::endl;
        if (code == -DER_NONEXIST)
            throw fdb5::DaosEntityNotFoundException(msg);
        if (code == -DER_EXIST)
            throw fdb5::DaosEntityAlreadyExistsException(msg);
        DaosManager::error(code, msg, file, line, func);
    }

    LOG_DEBUG_LIB(LibFdb5) << "DAOS_CALL <= " << msg << std::endl;

    return code;
}

//----------------------------------------------------------------------------------------------------------------------

/// @note: DaosSession acts as a mere wrapper for DaosManager such that DaosManager::instance does not need
///   to be called in so many places
/// @note: DaosSession no longer performs daos_init on creation and daos_fini on destroy. This is because
///   any pool handles obtained within a session are cached in DaosManager beyond DaosSession lifetime,
///   and the pool handles may become invalid if daos_fini is called for all sessions

class DaosSession {

public:  // methods

    DaosSession();

    DaosSession(const DaosSession&)            = delete;
    DaosSession& operator=(const DaosSession&) = delete;
    DaosSession(DaosSession&&)                 = delete;
    DaosSession& operator=(DaosSession&&)      = delete;

    ~DaosSession() {};

#ifdef fdb5_HAVE_DAOS_ADMIN
    // administrative
    fdb5::DaosPool& createPool(const uint64_t& scmSize = 10ULL << 30, const uint64_t& nvmeSize = 40ULL << 30);
    fdb5::DaosPool& createPool(const std::string& label, const uint64_t& scmSize = 10ULL << 30,
                               const uint64_t& nvmeSize = 40ULL << 30);
    void destroyPool(const fdb5::UUID&, const int& force = 1);
#endif

    fdb5::DaosPool& getPool(const fdb5::UUID&);
    fdb5::DaosPool& getPool(const std::string&);
    fdb5::DaosPool& getPool(const fdb5::UUID&, const std::string&);

    int containerOidsPerAlloc() const { return DaosManager::instance().containerOidsPerAlloc_; };
    uint64_t objectCreateCellSize() const { return DaosManager::instance().objectCreateCellSize_; };
    uint64_t objectCreateChunkSize() const { return DaosManager::instance().objectCreateChunkSize_; };

#ifdef fdb5_HAVE_DAOS_ADMIN
    std::string dmgConfigFile() const { return DaosManager::instance().dmgConfigFile_; };
#endif

private:  // methods

    PoolCache::iterator getCachedPool(const fdb5::UUID&);
    PoolCache::iterator getCachedPool(const std::string&);

private:  // members

    /// @todo: add lock_guards in all DaosSession methods using pool_cache_. Same for cont_cache_ in DaosPool.
    // std::mutex& mutex_;
    PoolCache& pool_cache_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
