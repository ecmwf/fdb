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

// TODO: including daos.h here indirectly.
//       Necessary due to the getHandle(). Find a way to avoid? 
//       These handles are not needed by the user in fact.
#include "fdb5/daos/DaosCluster.h"
#include "fdb5/daos/DaosObject.h"

namespace fdb5 {

class DaosPool;

class DaosContainer {

public:

    //TODO: address this
    DaosContainer();
    DaosContainer(fdb5::DaosPool*);
    DaosContainer(fdb5::DaosPool*, uuid_t);
    DaosContainer(fdb5::DaosPool*, std::string);
    ~DaosContainer();

    void create();
    void destroy();
    void open();
    void close();
    // TODO: AutoClose?

    std::string name();
    daos_handle_t& getHandle();

    fdb5::DaosObject* declareObject();
    fdb5::DaosObject* declareObject(daos_obj_id_t);

private:

    fdb5::DaosPool* pool_;
    uuid_t uuid_;
    bool known_uuid_;
    std::string label_;
    daos_handle_t coh_;
    bool open_;

};

}  // namespace fdb5