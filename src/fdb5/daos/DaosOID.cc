/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <sstream>
#include <iomanip>
#include <algorithm>

#include "eckit/exception/Exceptions.h"
#include "eckit/utils/Translator.h"
#include "eckit/utils/MD5.h"

#include "fdb5/daos/DaosOID.h"
#include "fdb5/daos/DaosContainer.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

void DaosOID::parseReservedBits() {

    otype_ = static_cast<enum daos_otype_t>((hi_ & OID_FMT_TYPE_MASK) >> OID_FMT_TYPE_SHIFT);
    oclass_ = (hi_ & OID_FMT_CLASS_MASK) >> OID_FMT_CLASS_SHIFT;

}

DaosOID::DaosOID(const uint64_t& hi, const uint64_t& lo) : hi_(hi), lo_(lo), wasGenerated_(true) {

    parseReservedBits();

}

DaosOID::DaosOID(const std::string& s) : wasGenerated_(true) {

    ASSERT(s.length() == 32);
    ASSERT(std::all_of(s.begin(), s.end(), ::isxdigit));

    hi_ = std::stoull(s.substr(0, 16), nullptr, 16);
    lo_ = std::stoull(s.substr(16, 16), nullptr, 16);

    parseReservedBits();

}

DaosOID::DaosOID(const uint32_t& hi, const uint64_t& lo, const enum daos_otype_t& otype, const daos_oclass_id_t& oclass) :
    otype_(otype), hi_(hi), lo_(lo), oclass_(oclass), wasGenerated_(false) {}

DaosOID::DaosOID(const std::string& name, const enum daos_otype_t& otype, const daos_oclass_id_t& oclass) :
    otype_(otype), oclass_(oclass), wasGenerated_(false) {

    eckit::MD5 md5(name);
    /// @todo: calculate digests of 12 bytes (24 hex characters) rather than 16 bytes (32 hex characters)
    ///        I have tried redefining MD5_DIGEST_LENGTH but it had no effect
    hi_ = std::stoull(md5.digest().substr(0, 8), nullptr, 16);
    lo_ = std::stoull(md5.digest().substr(8, 16), nullptr, 16);

}

// DaosOID::DaosOID(const DaosOID& other) : hi_(other.hi_), lo_(other.lo_) {}

// DaosOID::DaosOID(DaosOID&& other) {

//     std::swap(hi_, other.hi_);
//     std::swap(lo_, other.lo_);

// }

// DaosOID& DaosOID::operator=(DaosOID rhs) {

//     std::swap(*this, rhs);
//     return *this;

// }

void DaosOID::generate(fdb5::DaosContainer& cont) {

    if (wasGenerated_) return;
    hi_ = cont.generateOID(*this).asDaosObjIdT().hi;
    wasGenerated_ = true;

}

std::string DaosOID::asString() const {

    ASSERT(wasGenerated_);

    std::stringstream os;
    os << std::setw(16) << std::setfill('0') << std::hex << hi_;
    os << std::setw(16) << std::setfill('0') << std::hex << lo_;
    return os.str();

}

daos_obj_id_t DaosOID::asDaosObjIdT() const {

    return daos_obj_id_t{hi_, lo_};

}

enum daos_otype_t DaosOID::otype() const {

    return otype_;

}

daos_oclass_id_t DaosOID::oclass() const {

    return oclass_;

}

DaosArrayOID::DaosArrayOID(const uint64_t& hi, const uint64_t& lo) : DaosOID(hi, lo) { ASSERT(otype_ == DAOS_OT_ARRAY); }

DaosArrayOID::DaosArrayOID(const std::string& oid) : DaosOID(oid) { ASSERT(otype_ == DAOS_OT_ARRAY); }

DaosArrayOID::DaosArrayOID(const uint32_t& hi, const uint64_t& lo, const daos_oclass_id_t& oclass) :
    DaosOID(hi, lo, DAOS_OT_ARRAY, oclass) {}

DaosArrayOID::DaosArrayOID(const std::string& name, const daos_oclass_id_t& oclass) :
    DaosOID(name, DAOS_OT_ARRAY, oclass) {}

DaosKeyValueOID::DaosKeyValueOID(const uint64_t& hi, const uint64_t& lo) : DaosOID(hi, lo) { ASSERT(otype_ == DAOS_OT_KV_HASHED); }

DaosKeyValueOID::DaosKeyValueOID(const std::string& oid) : DaosOID(oid) { ASSERT(otype_ == DAOS_OT_KV_HASHED); }

DaosKeyValueOID::DaosKeyValueOID(const uint32_t& hi, const uint64_t& lo, const daos_oclass_id_t& oclass) :
    DaosOID(hi, lo, DAOS_OT_KV_HASHED, oclass) {}

DaosKeyValueOID::DaosKeyValueOID(const std::string& name, const daos_oclass_id_t& oclass) :
    DaosOID(name, DAOS_OT_KV_HASHED, oclass) {}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
