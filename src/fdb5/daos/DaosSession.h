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

#include "fdb5/daos/DaosPool.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

#define DAOS_CALL(a) fdb5::daos_call(a, #a, __FILE__, __LINE__, __func__)

class DaosSession : eckit::NonCopyable {

public: // methods

    DaosSession();
    ~DaosSession();

    fdb5::DaosPool& declarePool(uuid_t);
    fdb5::DaosPool& declarePool(const std::string&);
    fdb5::DaosPool& declarePool(uuid_t, const std::string&);

    fdb5::DaosPool& createPool();
    fdb5::DaosPool& createPool(const std::string& label);

    void destroyPool(uuid_t);

    void closePool(uuid_t);

    static void error(int code, const char* msg, const char* file, int line, const char* func);

private: // methods

    std::deque<fdb5::DaosPool>::iterator getCachedPool(uuid_t);
    std::deque<fdb5::DaosPool>::iterator getCachedPool(const std::string&);

public: //members

    static const daos_size_t default_pool_create_scm_size = 10ULL << 30;
    static const daos_size_t default_pool_create_nvme_size = 40ULL << 30;
    static const int default_pool_destroy_force = 1;

    static const int default_container_oids_per_alloc = 100;

    static const daos_size_t default_object_create_cell_size = 1;
    static const daos_size_t default_object_create_chunk_size = 1048576;

private: // members

    std::deque<fdb5::DaosPool> pool_cache_;

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