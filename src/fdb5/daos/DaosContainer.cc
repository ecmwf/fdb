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

#include "fdb5/daos/DaosContainer.h"
#include "fdb5/daos/DaosPool.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/TmpDir.h"

namespace fdb5 {

// TODO: adddress this.
DaosContainer::DaosContainer() {}

DaosContainer::DaosContainer(fdb5::DaosPool* pool) : pool_(pool), known_uuid_(false), open_(false) {}

DaosContainer::DaosContainer(fdb5::DaosPool* pool, uuid_t container_uuid) : pool_(pool), known_uuid_(true), open_(false) {

    uuid_copy(uuid_, container_uuid);

}

DaosContainer::DaosContainer(fdb5::DaosPool* pool, std::string container_label) : pool_(pool), known_uuid_(false), label_(container_label), open_(false) {}

DaosContainer::~DaosContainer() {

    if (open_) close();

}

void DaosContainer::create() {

    // TODO: not sure what to do here. Should probably keep track of whether 
    //       the container has been created or not via this DaosContainer instance,
    //       and return accordingly. But the container may be destroyed by another 
    //       process or DaosContainer instance.
    if (open_) return;

    if (!known_uuid_ && label_.size() == 0) {

        std::string random_name;

        random_name = eckit::TmpDir().baseName().path();
        random_name += "_" + std::to_string(getpid());

        const char *random_name_cstr = random_name.c_str();

        uuid_t seed = {0};

        uuid_generate_md5(uuid_, seed, random_name_cstr, strlen(random_name_cstr));
        known_uuid_ = true;

    }

    daos_handle_t poh = pool_->getHandle();

    if (known_uuid_) {

        DAOS_CALL(daos_cont_create(poh, uuid_, NULL, NULL));
        
    } else {

        DAOS_CALL(daos_cont_create_with_label(poh, label_.c_str(), NULL, NULL, NULL));
        
    }

}

void DaosContainer::destroy() {

    NOTIMP;

}

void DaosContainer::open() {

    if (open_) return;

    if (!known_uuid_ && label_.size() == 0) throw eckit::Exception("Cannot attempt connecting to an unidentified container. Either create it or provide a UUID or label upon construction.");

    if (label_.size() > 0) {

        DAOS_CALL(daos_cont_open(pool_->getHandle(), label_.c_str(), DAOS_COO_RW, &coh_, NULL, NULL));

    } else {

        DAOS_CALL(daos_cont_open(pool_->getHandle(), uuid_, DAOS_COO_RW, &coh_, NULL, NULL));

    }
    
    open_ = true;

}

void DaosContainer::close() {

    if (!open_) {
        eckit::Log::warning() << "Closing DaosContainer " << name() << ", container is not open" << std::endl;
        return;
    }
    
    DAOS_CALL(daos_cont_close(coh_, NULL));
    open_ = false;

}

std::string DaosContainer::name() {

    ASSERT(label_.size() > 0 || known_uuid_, "Cannot generate a name for an unidentified container. Either create it or provide a UUID or label upon construction.");

    if (label_.size() > 0) return pool_->name() + ":" + label_;

    char name_cstr[37];
    uuid_unparse(uuid_, name_cstr);
    return pool_->name() + ":" + std::string(name_cstr);

}

daos_handle_t& DaosContainer::getHandle() {
    
    if (open_) return coh_;
    throw eckit::Exception("Cannot get handle of unopened container.");
    
};

DaosPool* DaosContainer::getPool() {

    return pool_;

}

fdb5::DaosObject* DaosContainer::declareObject() {

    return new fdb5::DaosObject(this);

}

fdb5::DaosObject* DaosContainer::declareObject(daos_obj_id_t oid) {

    return new fdb5::DaosObject(this, oid);

}

}  // namespace fdb5