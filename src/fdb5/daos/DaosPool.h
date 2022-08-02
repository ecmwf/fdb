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

#include <string>

#include <uuid.h>
#include <daos/tests_lib.h>

#include "fdb5/daos/DaosContainer.h"

namespace fdb5 {

class DaosPool {

public:

    DaosPool();
    DaosPool(std::string);
    DaosPool(uuid_t);
    DaosPool(std::string, uuid_t);
    ~DaosPool();

    // administrative
    void create();
    void destroy();

    void open();
    void close();
    // TODO: AutoClose?

    std::string name();
    daos_handle_t& getHandle();

    fdb5::DaosContainer* declareContainer();
    fdb5::DaosContainer* declareContainer(uuid_t);
    fdb5::DaosContainer* declareContainer(std::string);

    static const daos_size_t default_create_scm_size = 10ULL << 30;
    static const daos_size_t default_create_nvme_size = 40ULL << 30;
    static const int default_destroy_force = 1;

private:

    uuid_t uuid_;
    bool known_uuid_;
    std::string label_;
    daos_handle_t poh_;
    bool open_;

};

}  // namespace fdb5