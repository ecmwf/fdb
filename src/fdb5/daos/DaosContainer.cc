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

#include "fdb5/LibFdb5.h"
#include "fdb5/daos/DaosContainer.h"
#include "fdb5/daos/DaosOID.h"
#include "fdb5/daos/DaosObject.h"
#include "fdb5/daos/DaosPool.h"
#include "fdb5/daos/DaosSession.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosContainer::DaosContainer(DaosContainer&& other) noexcept :
    pool_(other.pool_),
    label_(std::move(other.label_)),
    coh_(std::move(other.coh_)),
    open_(other.open_),
    oidAlloc_(std::move(other.oidAlloc_)) {

    other.open_ = false;
}

/// @todo: should forbid labels with UUID format?
DaosContainer::DaosContainer(fdb5::DaosPool& pool, const std::string& label) :
    pool_(pool), label_(label), open_(false) {}

DaosContainer::~DaosContainer() {

    /// @todo: in destroy() and close() wed want to close and destroy/invalidate all object instances for
    ///   objects in the container.
    ///   What happens if we do obj.open() and then cont.close()?

    if (open_) {
        close();
    }
}

void DaosContainer::create() {

    /// @todo: not sure what to do here. Should probably keep track of whether
    //       the container has been created or not via this DaosContainer instance,
    //       and return accordingly. But the container may have been destroyed by another
    //       process or DaosContainer instance.

    if (open_) {
        return;
    }

    ASSERT(label_.size() > 0);

    const daos_handle_t& poh = pool_.getOpenHandle();

    try {

        DAOS_CALL(daos_cont_create_with_label(poh, label_.c_str(), NULL, NULL, NULL));
    }
    catch (fdb5::DaosEntityAlreadyExistsException& e) {
    }
}

void DaosContainer::open() {

    if (open_) {
        return;
    }

    ASSERT(label_.size() > 0);

    const daos_handle_t& poh = pool_.getOpenHandle();

    DAOS_CALL(daos_cont_open(poh, label_.c_str(), DAOS_COO_RW, &coh_, NULL, NULL));

    open_ = true;
}

void DaosContainer::close() {

    if (!open_) {
        eckit::Log::warning() << "Closing DaosContainer " << name() << ", container is not open" << std::endl;
        return;
    }

    LOG_DEBUG_LIB(LibFdb5) << "DAOS_CALL => daos_cont_close()" << std::endl;

    int code = daos_cont_close(coh_, NULL);

    if (code < 0) {
        eckit::Log::warning() << "DAOS error in call to daos_cont_close(), file " << __FILE__ << ", line " << __LINE__
                              << ", function " << __func__ << " [" << code << "] (" << code << ")" << std::endl;
    }

    LOG_DEBUG_LIB(LibFdb5) << "DAOS_CALL <= daos_cont_close()" << std::endl;

    open_ = false;
}

uint64_t DaosContainer::allocateOIDLo() {

    open();

    if (oidAlloc_.num_oids == 0) {
        oidAlloc_.num_oids = fdb5::DaosSession().containerOidsPerAlloc();
        DAOS_CALL(daos_cont_alloc_oids(coh_, oidAlloc_.num_oids + 1, &(oidAlloc_.next_oid), NULL));
    }
    else {
        ++oidAlloc_.next_oid;
        --oidAlloc_.num_oids;
    }

    return oidAlloc_.next_oid;
}

fdb5::DaosArray DaosContainer::createArray(const daos_oclass_id_t& oclass, bool with_attr) {

    daos_otype_t otype = DAOS_OT_ARRAY;

    if (!with_attr) {
        otype = DAOS_OT_ARRAY_BYTE;
    }

    fdb5::DaosOID new_oid{0, allocateOIDLo(), otype, oclass};
    new_oid.generateReservedBits(*this);

    open();

    fdb5::DaosArray obj(*this, new_oid, false);
    obj.create();
    return obj;
}

fdb5::DaosArray DaosContainer::createArray(const fdb5::DaosOID& oid) {

    ASSERT(oid.otype() == DAOS_OT_ARRAY || oid.otype() == DAOS_OT_ARRAY_BYTE);

    open();

    fdb5::DaosArray obj(*this, oid, false);
    obj.create();
    return obj;
}

fdb5::DaosKeyValue DaosContainer::createKeyValue(const daos_oclass_id_t& oclass) {

    fdb5::DaosOID new_oid{0, allocateOIDLo(), DAOS_OT_KV_HASHED, oclass};
    new_oid.generateReservedBits(*this);

    open();

    fdb5::DaosKeyValue obj(*this, new_oid, false);
    obj.create();
    return obj;
}

fdb5::DaosKeyValue DaosContainer::createKeyValue(const fdb5::DaosOID& oid) {

    ASSERT(oid.otype() == DAOS_OT_KV_HASHED);

    open();

    fdb5::DaosKeyValue obj(*this, oid, false);
    obj.create();
    return obj;
}

std::vector<fdb5::DaosOID> DaosContainer::listOIDs() {

    /// @todo: proper memory management
    /// @todo: auto snap destroyer
    daos_epoch_t e;
    DAOS_CALL(daos_cont_create_snap_opt(coh_, &e, NULL, (enum daos_snapshot_opts)(DAOS_SNAP_OPT_CR | DAOS_SNAP_OPT_OIT),
                                        NULL));

    daos_handle_t oith;
    DAOS_CALL(daos_oit_open(coh_, e, &oith, NULL));

    std::vector<fdb5::DaosOID> oids;
    daos_anchor_t anchor = DAOS_ANCHOR_INIT;

    uint32_t max_oids_per_rpc = 10;  /// @todo: take from config
    std::vector<daos_obj_id_t> oid_batch(max_oids_per_rpc);

    while (!daos_anchor_is_eof(&anchor)) {
        uint32_t oids_nr = max_oids_per_rpc;
        DAOS_CALL(daos_oit_list(oith, oid_batch.data(), &oids_nr, &anchor, NULL));
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

    try {

        open();
    }
    catch (fdb5::DaosEntityNotFoundException& e) {

        return false;
    }

    return true;
}

std::string DaosContainer::name() const {

    return label_;
}

std::string DaosContainer::label() const {

    return label_;
}

fdb5::DaosPool& DaosContainer::getPool() const {

    return pool_;
}

AutoContainerDestroy::~AutoContainerDestroy() noexcept(false) {

    bool fail = !eckit::Exception::throwing();

    try {
        cont_.getPool().destroyContainer(cont_.label());
    }
    catch (std::exception& e) {
        eckit::Log::error() << "** " << e.what() << " Caught in " << Here() << std::endl;
        if (fail) {
            eckit::Log::error() << "** Exception is re-thrown" << std::endl;
            throw;
        }
        eckit::Log::error() << "** An exception is already in progress" << std::endl;
        eckit::Log::error() << "** Exception is ignored" << std::endl;
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
