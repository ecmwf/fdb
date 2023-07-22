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

    DaosContainer(DaosContainer&&) noexcept;
    ~DaosContainer();

    // DaosContainer& operator=(DaosContainer&&) noexcept;

    void open();
    void close();

    uint64_t allocateOIDLo();
    fdb5::DaosOID generateOID(const fdb5::DaosOID&);
    fdb5::DaosArray createArray(const daos_oclass_id_t& oclass = OC_S1);
    fdb5::DaosArray createArray(const fdb5::DaosOID&);
    fdb5::DaosKeyValue createKeyValue(const daos_oclass_id_t& oclass = OC_S1);
    fdb5::DaosKeyValue createKeyValue(const fdb5::DaosOID&);
    std::vector<fdb5::DaosOID> listOIDs();

    const daos_handle_t& getOpenHandle();

    std::string name() const;
    void uuid(uuid_t) const;
    std::string label() const;
    fdb5::DaosPool& getPool() const;

private: // methods

    friend class DaosPool;

    DaosContainer(fdb5::DaosPool&);
    DaosContainer(fdb5::DaosPool&, uuid_t);
    DaosContainer(fdb5::DaosPool&, const std::string&);
    DaosContainer(fdb5::DaosPool&, uuid_t, const std::string&);

    void create();

    bool exists();

private: // members

    fdb5::DaosPool& pool_;
    uuid_t uuid_;
    bool known_uuid_;
    std::string label_ = std::string();
    daos_handle_t coh_;
    bool open_;

    OidAlloc oid_alloc_{};

};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
