/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/TmpDir.h"

#include "fdb5/daos/DaosCluster.h"
#include "fdb5/daos/DaosPool.h"
#include "fdb5/daos/DaosContainer.h"
#include "fdb5/daos/DaosObject.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosContainer::DaosContainer(fdb5::DaosPool& pool, uuid_t uuid) : pool_(pool), known_uuid_(true), open_(false) {

    uuid_copy(uuid_, uuid);

}

DaosContainer::DaosContainer(fdb5::DaosPool& pool, const std::string& label) : pool_(pool), known_uuid_(false), label_(label), open_(false) {}

DaosContainer::DaosContainer(fdb5::DaosPool& pool, uuid_t uuid, const std::string& label) : pool_(pool), known_uuid_(true), label_(label), open_(false) {

    uuid_copy(uuid_, uuid);

}

DaosContainer::~DaosContainer() {

    if (open_) close();

}

void DaosContainer::create() {

    // TODO: not sure what to do here. Should probably keep track of whether 
    //       the container has been created or not via this DaosContainer instance,
    //       and return accordingly. But the container may be destroyed by another 
    //       process or DaosContainer instance.
    if (open_) return;

    ASSERT(known_uuid_ || label_.size() > 0);
    // if (!known_uuid_ && label_.size() == 0) {

    //     std::string random_name;

    //     random_name = eckit::TmpDir().baseName().path();
    //     random_name += "_" + std::to_string(getpid());

    //     const char *random_name_cstr = random_name.c_str();

    //     uuid_t seed = {0};

    //     uuid_generate_md5(uuid_, seed, random_name_cstr, strlen(random_name_cstr));
    //     known_uuid_ = true;

    // }

    const daos_handle_t& poh = pool_.getOpenHandle();

    if (known_uuid_) {

        DAOS_CALL(daos_cont_create(poh, uuid_, NULL, NULL));
        
    } else {

        DAOS_CALL(daos_cont_create_with_label(poh, label_.c_str(), NULL, NULL, NULL));
        
    }

}

void DaosContainer::destroy() {

    NOTIMP;

    // if (known_uuid_) pool_.closeContainer(uuid_);
    // if (label_.size() > 0) pool_.closeContainer(label_);

    // daos_cont_destroy

    // TODO: this results in an invalid DaosContainer instance. Address as in DaosPool::destroy().

}

void DaosContainer::open() {

    if (open_) return;

    // if (!known_uuid_ && label_.size() == 0) throw eckit::Exception("Cannot attempt connecting to an unidentified container. Either create it or provide a UUID or label upon construction.");

    const daos_handle_t& poh = pool_.getOpenHandle();

    if (known_uuid_) {

        DAOS_CALL(daos_cont_open(poh, uuid_, DAOS_COO_RW, &coh_, NULL, NULL));

    } else {

        DAOS_CALL(daos_cont_open(poh, label_.c_str(), DAOS_COO_RW, &coh_, NULL, NULL));

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

fdb5::DaosObject DaosContainer::createObject() {

    open();

    if (oid_alloc_.num_oids == 0) {
        oid_alloc_.num_oids = fdb5::DaosCluster::default_container_oids_per_alloc;
        DAOS_CALL(daos_cont_alloc_oids(coh_, oid_alloc_.num_oids + 1, &(oid_alloc_.next_oid), NULL));
    } else {
        ++oid_alloc_.next_oid;
        --oid_alloc_.num_oids;
    }

    daos_obj_id_t next;
    next.lo = oid_alloc_.next_oid;
    DAOS_CALL(daos_array_generate_oid(coh_, &next, true, OC_S1, 0, 0));

    fdb5::DaosObject obj(*this, next);
    obj.create();
    return obj;

}

fdb5::DaosObject DaosContainer::createObject(daos_obj_id_t oid) {

    fdb5::DaosObject obj(*this, oid);
    obj.create();
    return obj;

}

fdb5::DaosObject DaosContainer::createObject(const std::string& oid) {

    // TODO: assert in container creation that the provided label has size > 0?

    fdb5::DaosObject obj(*this, oid);
    obj.create();
    return obj;

}

const daos_handle_t& DaosContainer::getOpenHandle() {
    
    open();
    return coh_;
    
};

std::string DaosContainer::name() const {

    if (label_.size() > 0) return label_;

    char name_cstr[37];
    uuid_unparse(uuid_, name_cstr);
    return std::string(name_cstr);

}

void DaosContainer::uuid(uuid_t uuid) const {

    uuid_copy(uuid, uuid_);

}

std::string DaosContainer::label() const {

    return label_;

}

fdb5::DaosPool& DaosContainer::getPool() const {

    return pool_;

}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5