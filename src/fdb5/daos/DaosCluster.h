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
///
/// @date Jul 2022

#pragma once

#include <iostream>
#include <string>
#include <iomanip>
#include <sstream>
#include <map>

#include <daos.h>

#include "eckit/exception/Exceptions.h"

#define OIDS_PER_ALLOC 1024

namespace fdb5 {

// TODO: really necessary?
// TODO: naming? oid_to_string
std::string oidToStr(const daos_obj_id_t&);
bool strToOid(const std::string&, daos_obj_id_t*);

class DaosContainer;

// struct OidAlloc;

#define DAOS_CALL(a) fdb5::daos_call(a, #a, __FILE__, __LINE__, __func__)

class DaosCluster {

public:

    static DaosCluster& instance();

    daos_obj_id_t getNextOid(fdb5::DaosContainer*);

    static void error(int code, const char* msg, const char* file, int line, const char* func);

private:

    DaosCluster();
    ~DaosCluster();

    // typedef std::map< std::string, OidAlloc * > SinglePoolOidAllocStore;
    // typedef std::map< std::string, SinglePoolOidAllocStore * > OidAllocStore;
    // mutable OidAllocStore oid_allocs_;

};

static inline int daos_call(int code, const char* msg, const char* file, int line, const char* func) {

    std::cout << "DAOS_CALL => " << msg << std::endl;

    if (code < 0) {
        std::cout << "DAOS_FAIL !! " << msg << std::endl;
        DaosCluster::error(code, msg, file, line, func);
    }

    std::cout << "DAOS_CALL <= " << msg << std::endl;

    return code;
}

}  // namespace fdb5