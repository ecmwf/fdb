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

#include "fdb5/daos/DaosPool.h"
#include "eckit/io/Offset.h"
#include "eckit/filesystem/URI.h"

namespace fdb5 {

class DaosContainer;

class DaosObject {

public:

    //TODO: address this
    DaosObject();
    DaosObject(fdb5::DaosPool*);
    DaosObject(fdb5::DaosContainer*);
    DaosObject(fdb5::DaosContainer*, daos_obj_id_t);
    DaosObject(fdb5::DaosContainer*, std::string);
    DaosObject(std::string title);
    DaosObject(std::string pool, std::string cont);
    DaosObject(std::string pool, std::string cont, std::string oid);
    DaosObject(eckit::URI);
    ~DaosObject();

    void create();
    void destroy();
    // exists
    // owner
    // empty
    daos_size_t size();

    std::string name();
    eckit::URI URI();

    eckit::DataHandle* daosHandle(bool overwrite = false) const;

    void open();
    void close();
    // TODO: AutoClose?
    // TODO: rename to handle()
    daos_handle_t& getHandle();
    DaosContainer* getContainer();
    DaosPool* getPool();

    long write(const void*, long, eckit::Offset);
    long read(void*, long, eckit::Offset);

    static const daos_size_t default_create_cell_size = 1;
    static const daos_size_t default_create_chunk_size = 1048576;

private:

    fdb5::DaosContainer* cont_;
    daos_obj_id_t oid_;
    bool known_oid_;
    daos_handle_t oh_;
    bool open_;

    void construct(std::string& title);

};

}  // namespace fdb5