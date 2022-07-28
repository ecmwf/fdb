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

#include "eckit/io/Offset.h"

namespace fdb5 {

class DaosContainer;

// TODO: redistribute things. We should have an equivalent to PathName (description, DaosObject) and FileHandle (actions - open, write), DaosHandle)
class DaosObject {

public:

    //TODO: address this
    DaosObject();
    DaosObject(fdb5::DaosContainer*);
    DaosObject(fdb5::DaosContainer*, daos_obj_id_t);
    ~DaosObject();

    void create();
    void destroy();
    void open();
    void close();
    // TODO: AutoClose?
    long write(const void*, long, eckit::Offset);
    long read(void*, long, eckit::Offset);

    daos_size_t getSize();
    std::string name();
    daos_handle_t& getHandle();

    static const daos_size_t default_create_cell_size = 1;
    static const daos_size_t default_create_chunk_size = 1048576;

private:

    fdb5::DaosContainer* cont_;
    daos_obj_id_t oid_;
    bool known_oid_;
    daos_handle_t oh_;
    bool open_;

};

}  // namespace fdb5