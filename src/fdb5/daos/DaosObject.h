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

#include "eckit/filesystem/URI.h"

#include "fdb5/daos/DaosOID.h"
#include "fdb5/daos/DaosName.h"

// #include "eckit/io/Offset.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

// class DaosPool;

class DaosContainer;

class DaosSession;

fdb5::DaosContainer& name_to_cont_ref(fdb5::DaosSession&, const fdb5::DaosName&);

class DaosObject {

public: // methods

    DaosObject(fdb5::DaosContainer&, const fdb5::DaosOID&);
    DaosObject(fdb5::DaosSession&, const fdb5::DaosName&);
    DaosObject(fdb5::DaosSession&, const eckit::URI&);
    DaosObject(DaosObject&&) noexcept;
    ~DaosObject();

    void destroy();
    daos_size_t size();

    void open();
    void close();

    const daos_handle_t& getOpenHandle();

    std::string name() const;
    fdb5::DaosOID OID() const;
    eckit::URI URI() const;
    fdb5::DaosContainer& getContainer() const;

    long write(const void*, long, eckit::Offset);
    long read(void*, long, eckit::Offset);

private: // methods

    friend DaosContainer;

    DaosObject(fdb5::DaosContainer&, const fdb5::DaosOID&, bool verify);

    void create();

    bool exists();

private: // members

    fdb5::DaosContainer& cont_;
    fdb5::DaosOID oid_;
    daos_handle_t oh_;
    bool open_;

};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5