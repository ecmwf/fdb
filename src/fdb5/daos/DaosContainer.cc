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

#include "fdb5/daos/DaosSession.h"
#include "fdb5/daos/DaosPool.h"
#include "fdb5/daos/DaosContainer.h"
#include "fdb5/daos/DaosObject.h"
#include "fdb5/daos/DaosOID.h"

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

    /// @todo: AND IN DESTROY() AND CLOSE() WED WANT TO CLOSE AND DESTROY/INVALIDATE ALL OBJECT INSTANCES FOR OBJECTS IN THE CONT
    // WHAT HAPPENS IF WE DO OBJ.OPEN AND THEN CONT.CLOSE???

    if (open_) close();

}

void DaosContainer::create() {

    /// @todo: not sure what to do here. Should probably keep track of whether 
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

    ASSERT(label_.size() > 0);

    if (known_uuid_) pool_.closeContainer(uuid_);
        
    pool_.closeContainer(label_);

    const daos_handle_t& poh = pool_.getOpenHandle();

    DAOS_CALL(daos_cont_destroy(poh, label_.c_str(), 1, NULL));

    /// @todo: this results in an invalid DaosContainer instance. Address as in DaosPool::destroy().
    /// @todo: flag instance as invalid / non-existing? not allow open() anymore if instance is invalid. Assert in all Object actions that cont is valid
    /// @todo: STILL, WHENEVER A POOL/CONT IS DELETED AND THE USER OWNS OPEN OBJECTS, THEIR HANDLES MAY NOT BE POSSIBLE TO CLOSE ANYMORE, AND ANY ACTIONS ON SUCH
    // OBJECTS WILL FAIL WITH A WEIRD DAOS ERROR

}

void DaosContainer::open() {

    if (open_) return;

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
    
    std::cout << "DAOS_CALL => daos_cont_close()" << std::endl;

    int code = daos_cont_close(coh_, NULL);

    if (code < 0) eckit::Log::warning() << "DAOS error in call to daos_cont_close(), file " 
        << __FILE__ << ", line " << __LINE__ << ", function " << __func__ << " [" << code << "] (" 
        << strerror(-code) << ")" << std::endl;
        
    std::cout << "DAOS_CALL <= daos_cont_close()" << std::endl;

    open_ = false;

}

fdb5::DaosObject DaosContainer::createObject() {

    create();
    open();

    if (oid_alloc_.num_oids == 0) {
        oid_alloc_.num_oids = getPool().getSession().containerOidsPerAlloc();
        DAOS_CALL(daos_cont_alloc_oids(coh_, oid_alloc_.num_oids + 1, &(oid_alloc_.next_oid), NULL));
    } else {
        ++oid_alloc_.next_oid;
        --oid_alloc_.num_oids;
    }

    daos_obj_id_t next;
    next.lo = oid_alloc_.next_oid;
    DAOS_CALL(daos_array_generate_oid(coh_, &next, true, OC_S1, 0, 0));

    fdb5::DaosObject obj(*this, fdb5::DaosOID{next.hi, next.lo}, false);
    obj.create();
    return obj;

}

fdb5::DaosObject DaosContainer::createObject(uint32_t hi, uint64_t lo) {

    create();
    open();

    daos_obj_id_t id{(uint64_t) hi, lo};
    DAOS_CALL(daos_array_generate_oid(coh_, &id, true, OC_S1, 0, 0));
    
    fdb5::DaosObject obj(*this, fdb5::DaosOID{id.hi, id.lo}, false);
    obj.create();
    return obj;

}

const daos_handle_t& DaosContainer::getOpenHandle() {
    
    open();
    return coh_;
    
};

bool DaosContainer::exists() {

    /// @todo: implement this with more appropriate DAOS API functions
    try {

        open();

    } catch (eckit::SeriousBug& e) {

        return false;

    }
    
    return true;

}

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