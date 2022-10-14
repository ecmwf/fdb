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

#include "fdb5/daos/DaosName.h"

// #include "eckit/io/Offset.h"

daos_obj_id_t str_to_oid(const std::string&);
std::string oid_to_str(const daos_obj_id_t&);
fdb5::DaosContainer& name_to_cont_ref(const fdb5::DaosName&);

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

// class DaosPool;

class DaosContainer;

class DaosObject {

public: // methods

    DaosObject(fdb5::DaosContainer&, daos_obj_id_t);
    DaosObject(fdb5::DaosContainer&, const std::string&);
    DaosObject(const fdb5::DaosName&);
    DaosObject(const eckit::URI&);
    ~DaosObject();

    void create();
    void destroy();
//     // exists
//     // owner
//     // empty
    daos_size_t size();

    void open();
    void close();
//     // TODO: AutoClose?

    const daos_handle_t& getOpenHandle();

    fdb5::DaosName name() const;
    eckit::URI URI() const;
    fdb5::DaosContainer& getContainer() const;

    long write(const void*, long, eckit::Offset);
    long read(void*, long, eckit::Offset);

private: // members

    fdb5::DaosContainer& cont_;
    daos_obj_id_t oid_;
    daos_handle_t oh_;
    bool open_;

};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5