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

#include "fdb5/daos/DaosCluster.h"
#include "fdb5/daos/DaosContainer.h"

namespace fdb5 {

std::string oidToStr(const daos_obj_id_t& oid) {
    std::stringstream os;
    os << std::setw(16) << std::setfill('0') << std::hex << oid.hi << ".";
    os << std::setw(16) << std::setfill('0') << std::hex << oid.lo;
    return os.str();
}

bool strToOid(const std::string& x, daos_obj_id_t *oid) {
    if (x.length() != 33)
        return false;
    // TODO: do better checks and avoid copy
    std::string y(x);
    y[16] = '0';
    if (!std::all_of(y.begin(), y.end(), ::isxdigit))
        return false;

    std::string hi = x.substr(0, 16);
    std::string lo = x.substr(17, 16);
    oid->hi = std::stoull(hi, nullptr, 16);
    oid->lo = std::stoull(lo, nullptr, 16);

    return true;
}

// struct OidAlloc {
//     OidAlloc() : num_oids_(0) {}
//     uint64_t next_oid_;
//     int num_oids_;
// };

DaosCluster& DaosCluster::instance() {
    static DaosCluster instance_;
    return instance_;
}

DaosCluster::DaosCluster() {

    DAOS_CALL(daos_init());

}

DaosCluster::~DaosCluster() {

    DAOS_CALL(daos_fini());

}

daos_obj_id_t DaosCluster::getNextOid(DaosContainer* cont) {

    // OidAlloc *alloc = nullptr;

    // auto j = oid_allocs_.find(pool);
    // if (j != oid_allocs_.end()) {
    //     auto k = j->second->find(cont);
    //     if (k != j->second->end())
    //         alloc = k->second;
    // }

    // if (!alloc) {
    //     oid_allocs_[pool][0][cont] = new OidAlloc();
    //     alloc = oid_allocs_[pool][0][cont];
    // }

    // TODO: improve this
    cont->create();

    cont->open();

    daos_handle_t coh;
    coh = cont->getHandle();

    // if (alloc->num_oids_ == 0) {
    //     alloc->num_oids_ = OIDS_PER_ALLOC;
    //     DAOS_CALL(daos_cont_alloc_oids(coh, alloc->num_oids_ + 1, &(alloc->next_oid_), NULL));
    // } else {
    //     alloc->next_oid_++;
    //     alloc->num_oids_--;
    // }

    // TODO: move to container and/or array
    daos_obj_id_t next;
    DAOS_CALL(daos_cont_alloc_oids(coh, (daos_size_t) 1, &(next.lo), NULL));
    DAOS_CALL(daos_array_generate_oid(coh, &next, true, OC_S1, 0, 0));

    // next.lo = alloc->next_oid_;
    // DAOS_CALL(daos_array_generate_oid(coh, &next, true, OC_S1, 0, 0));

    // TODO: think about connecting and disconnecting pools
    //pool->disconnect();

    return next;
}

void DaosCluster::error(int code, const char* msg, const char* file, int line, const char* func) {

    std::ostringstream oss;
    oss << "DAOS error " << msg << ", file " << file << ", line " << line << ", function " << func << " [" << code
        << "] (" << strerror(-code) << ")";
    throw eckit::SeriousBug(oss.str());
}

}  // namespace fdb5