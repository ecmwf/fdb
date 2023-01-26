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
#include "fdb5/config/LocalConfiguration.h"

#include "fdb5/daos/DaosPool.h"

namespace fdb5 {


class DaosManager : private eckit::NonCopyable {

public: // methods

    // TODO: set configuration where relvant in DaosStore, DaosFieldLocation and in unit tests
    // TODO: unit tests for config
    static DaosManager& instance() {
        static DaosManager instance;
        return instance;
    };

    void configure(const eckit::LocalConfiguration& config) {
        containerOidsPerAlloc_ = config.getLong("defaultCellSize", containerOidsPerAlloc_);
        defaultCellSize_ = config.getLong("defaultCellSize", defaultCellSize_);
        chunkCreationSize_ = config.getLong("defaultCellSize", chunkCreationSize_);
    };

private: // member

    DaosManager() :
        containerOidsPerAlloc_(100),
        defaultCellSize_(1),
        chunkCreationSize_(1048576) {

        const Config& config = LibFdb5::defaultConfig();

        if (config.has("daos")) {
            auto daosConfig = config.getSubConfiguration("daos");
            configure(daosConfig);
        }
    }

public: //members

    // TODO: move these to proper config
    static const daos_size_t default_pool_create_scm_size = 10ULL << 30;
    static const daos_size_t default_pool_create_nvme_size = 40ULL << 30;
    static const int default_pool_destroy_force = 1;

private: // members

    long containerOidsPerAlloc_;
    long defaultCellSize_;
    long chunkCreationSize_;
};

//----------------------------------------------------------------------------------------------------------------------

#define DAOS_CALL(a) fdb5::daos_call(a, #a, __FILE__, __LINE__, __func__)

class DaosSession : eckit::NonCopyable {

public: // methods

    DaosSession(const eckit::LocalConfiguration& config = eckit::LocalConfiguration());
    ~DaosSession();

    // administrative
    fdb5::DaosPool& createPool();
    fdb5::DaosPool& createPool(const std::string& label);

    fdb5::DaosPool& getPool(uuid_t);
    fdb5::DaosPool& getPool(const std::string&);
    fdb5::DaosPool& getPool(uuid_t, const std::string&);

    void closePool(uuid_t);
    void destroyPoolContainers(uuid_t);

    static void error(int code, const char* msg, const char* file, int line, const char* func);

private: // methods

    /// @todo: Use std::map<std::string, fdb5::DaosPool>
    /// @todo: Offload caching to manager?
    using PoolCache = std::deque<fdb5::DaosPool>;
    PoolCache::iterator getCachedPool(uuid_t);
    PoolCache::iterator getCachedPool(const std::string&);

private: // members

    PoolCache pool_cache_;
    eckit::LocalConfiguration& config_;
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