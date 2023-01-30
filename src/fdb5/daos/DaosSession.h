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

#include "eckit/exception/Exceptions.h"
#include "eckit/config/LocalConfiguration.h"

#include "fdb5/daos/DaosPool.h"

namespace fdb5 {

class DaosManager : private eckit::NonCopyable {

public: // methods

    // TODO: set configuration where relvant in unit tests
    // TODO: unit tests for config
    static DaosManager& instance() {
        static DaosManager instance;        
        return instance;
    };

    void configure(const eckit::LocalConfiguration& config) {
        containerOidsPerAlloc_ = config.getInt("container_oids_per_alloc", containerOidsPerAlloc_);
        objectCreateCellSize_ = config.getInt64("object_create_cell_size", objectCreateCellSize_);
        objectCreateChunkSize_ = config.getInt64("object_create_chunk_size", objectCreateChunkSize_);
    };

    int containerOidsPerAlloc() const { return containerOidsPerAlloc_; };
    uint64_t objectCreateCellSize() const { return objectCreateCellSize_; };
    uint64_t objectCreateChunkSize() const { return objectCreateChunkSize_; };

private: // methods

    DaosManager() : 
        containerOidsPerAlloc_(100),
        objectCreateCellSize_(1),
        objectCreateChunkSize_(1048576) {}
        // TODO: should configure here with LibFdb5 default config, for cases where DaosManager is never configured?
        //       I would say No. fdb5 classes should always configure Daos including configuration from LibFdb5 default config.
        //       Daos* classes should not depend on fdb5 configuration if used independently.

private: // members

    int containerOidsPerAlloc_;
    uint64_t objectCreateCellSize_;
    uint64_t objectCreateChunkSize_;

};

//----------------------------------------------------------------------------------------------------------------------

#define DAOS_CALL(a) fdb5::daos_call(a, #a, __FILE__, __LINE__, __func__)

class DaosSession : eckit::NonCopyable {

public: // methods

    DaosSession(const eckit::LocalConfiguration& config = eckit::LocalConfiguration());
    ~DaosSession();

    // administrative
    fdb5::DaosPool& createPool(
        const uint64_t& scmSize = 10ULL << 30, 
        const uint64_t& nvmeSize = 40ULL << 30);
    fdb5::DaosPool& createPool(
        const std::string& label, 
        const uint64_t& scmSize = 10ULL << 30, 
        const uint64_t& nvmeSize = 40ULL << 30);

    fdb5::DaosPool& getPool(uuid_t);
    fdb5::DaosPool& getPool(const std::string&);
    fdb5::DaosPool& getPool(uuid_t, const std::string&);

    void closePool(uuid_t);
    void destroyPoolContainers(uuid_t);

    static void error(int code, const char* msg, const char* file, int line, const char* func);

    int containerOidsPerAlloc() const { return containerOidsPerAlloc_; };
    uint64_t objectCreateCellSize() const { return objectCreateCellSize_; };
    uint64_t objectCreateChunkSize() const { return objectCreateChunkSize_; };

private: // methods

    /// @todo: Use std::map<std::string, fdb5::DaosPool>
    /// @todo: Offload caching to manager?
    using PoolCache = std::deque<fdb5::DaosPool>;
    PoolCache::iterator getCachedPool(uuid_t);
    PoolCache::iterator getCachedPool(const std::string&);

private: // members

    PoolCache pool_cache_;

    int containerOidsPerAlloc_;
    uint64_t objectCreateCellSize_;
    uint64_t objectCreateChunkSize_;

};

static inline int daos_call(int code, const char* msg, const char* file, int line, const char* func) {

    std::cout << "DAOS_CALL => " << msg << std::endl;

    if (code < 0) {
        std::cout << "DAOS_FAIL !! " << msg << std::endl;
        DaosSession::error(code, msg, file, line, func);
    }

    std::cout << "DAOS_CALL <= " << msg << std::endl;

    return code;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5