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

DaosContainer::DaosContainer(DaosContainer&& other) noexcept : 
    pool_(other.pool_), known_uuid_(other.known_uuid_), 
    label_(std::move(other.label_)), coh_(std::move(other.coh_)), open_(other.open_),
    oid_alloc_(std::move(other.oid_alloc_)) {

    uuid_copy(uuid_, other.uuid_);
    other.open_ = false;

}

DaosContainer::DaosContainer(fdb5::DaosPool& pool) : pool_(pool), known_uuid_(false), open_(false) {}

DaosContainer::DaosContainer(fdb5::DaosPool& pool, uuid_t uuid) : pool_(pool), known_uuid_(true), open_(false) {

    uuid_copy(uuid_, uuid);

}

/// @todo: should forbid labels with UUID format?
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

    if (known_uuid_) {
        open();
        return;
    }

    const daos_handle_t& poh = pool_.getOpenHandle();

    if (label_.size() > 0) {

        DAOS_CALL(daos_cont_create_with_label(poh, label_.c_str(), NULL, &uuid_, NULL));

    } else {

        DAOS_CALL(daos_cont_create(poh, &uuid_, NULL, NULL));
        
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

    ASSERT(known_uuid_ || label_.size() > 0);

    const daos_handle_t& poh = pool_.getOpenHandle();

    if (label_.size() > 0) {
        DAOS_CALL(daos_cont_open(poh, label_.c_str(), DAOS_COO_RW, &coh_, NULL, NULL));
    } else {
        char uuid_cstr[37] = "";
        uuid_unparse(uuid_, uuid_cstr);
        DAOS_CALL(daos_cont_open(poh, uuid_cstr, DAOS_COO_RW, &coh_, NULL, NULL));
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
        << code << ")" << std::endl;
        
    std::cout << "DAOS_CALL <= daos_cont_close()" << std::endl;

    open_ = false;

}

uint64_t DaosContainer::allocateOIDLo() {

    open();

    if (oid_alloc_.num_oids == 0) {
        oid_alloc_.num_oids = getPool().getSession().containerOidsPerAlloc();
        DAOS_CALL(daos_cont_alloc_oids(coh_, oid_alloc_.num_oids + 1, &(oid_alloc_.next_oid), NULL));
    } else {
        ++oid_alloc_.next_oid;
        --oid_alloc_.num_oids;
    }

    return oid_alloc_.next_oid;

}

fdb5::DaosOID DaosContainer::generateOID(const fdb5::DaosOID& oid) {

    if (oid.wasGenerated()) return oid;

    open();

    daos_obj_id_t id(oid.asDaosObjIdT());
    DAOS_CALL(daos_obj_generate_oid(coh_, &id, oid.otype(), oid.oclass(), 0, 0));

    return fdb5::DaosOID{id.hi, id.lo};

}

fdb5::DaosArray DaosContainer::createArray(const daos_oclass_id_t& oclass) {

    fdb5::DaosOID new_oid = generateOID(fdb5::DaosOID{0, allocateOIDLo(), DAOS_OT_ARRAY, oclass});

    open();

    fdb5::DaosArray obj(*this, new_oid, false);
    obj.create();
    return obj;

}

fdb5::DaosArray DaosContainer::createArray(const fdb5::DaosOID& oid) {

    ASSERT(oid.otype() == DAOS_OT_ARRAY);

    open();

    fdb5::DaosArray obj(*this, generateOID(oid), false);
    obj.create();
    return obj;

}

fdb5::DaosKeyValue DaosContainer::createKeyValue(const daos_oclass_id_t& oclass) {

    fdb5::DaosOID new_oid = generateOID(fdb5::DaosOID{0, allocateOIDLo(), DAOS_OT_KV_HASHED, oclass});

    open();

    fdb5::DaosKeyValue obj(*this, new_oid, false);
    obj.create();
    return obj;

}

fdb5::DaosKeyValue DaosContainer::createKeyValue(const fdb5::DaosOID& oid) {

    ASSERT(oid.otype() == DAOS_OT_KV_HASHED);

    open();

    fdb5::DaosKeyValue obj(*this, generateOID(oid), false);
    obj.create();
    return obj;

}

std::vector<fdb5::DaosOID> DaosContainer::listOIDs() {
    
    /// @todo: proper memory management
    /// @todo: auto snap destroyer
    daos_epoch_t e;
    DAOS_CALL(
        daos_cont_create_snap_opt(
            coh_, &e, NULL, (enum daos_snapshot_opts)(DAOS_SNAP_OPT_CR | DAOS_SNAP_OPT_OIT), NULL
        )
    );

    daos_handle_t oith;
    DAOS_CALL(daos_oit_open(coh_, e, &oith, NULL));

    std::vector<fdb5::DaosOID> oids;
    daos_anchor_t anchor = DAOS_ANCHOR_INIT;
    int max_oids_per_rpc = 10;  /// @todo: take from config
    daos_obj_id_t oid_batch[max_oids_per_rpc];
    while (!daos_anchor_is_eof(&anchor)) {
        uint32_t oids_nr = max_oids_per_rpc;
        DAOS_CALL(daos_oit_list(oith, oid_batch, &oids_nr, &anchor, NULL));
        for (int i = 0; i < oids_nr; i++) {
            oids.push_back(fdb5::DaosOID{oid_batch[i].hi, oid_batch[i].lo});
        }
    }

    DAOS_CALL(daos_oit_close(oith, NULL));

    daos_epoch_range_t epr{e, e};
    DAOS_CALL(daos_cont_destroy_snap(coh_, epr, NULL));

    return oids;

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
