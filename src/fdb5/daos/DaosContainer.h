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

#include <uuid/uuid.h>
#include <daos.h>

#include <string>

#include "fdb5/daos/DaosObject.h"

struct OidAlloc {
    uint64_t next_oid;
    int num_oids;
};

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DaosPool;

class DaosContainer {

public: // methods

    ~DaosContainer();

    void create();
    void destroy();
    void open();
    void close();
    // TODO: AutoClose?

    fdb5::DaosObject createObject();
    fdb5::DaosObject createObject(daos_obj_id_t);
    fdb5::DaosObject createObject(const std::string& oid);

    const daos_handle_t& getOpenHandle();

    std::string name() const;
    void uuid(uuid_t) const;
    std::string label() const;
    fdb5::DaosPool& getPool() const;

private: // methods

    DaosContainer(fdb5::DaosPool&, uuid_t);
    DaosContainer(fdb5::DaosPool&, const std::string&);
    DaosContainer(fdb5::DaosPool&, uuid_t, const std::string&);
    friend class DaosPool;

private: // members

    fdb5::DaosPool& pool_;
    uuid_t uuid_;
    bool known_uuid_;
    std::string label_ = std::string();
    daos_handle_t coh_;
    bool open_;

    // TODO: is OidAlloc declared and defined properly?
    // TODO: is oid_alloc_ initialised in the cleanest/clearest way possible?
    // TODO: is it OK to enforce allocation of oid ranges?
    OidAlloc oid_alloc_{};

};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5