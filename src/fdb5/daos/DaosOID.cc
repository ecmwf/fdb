/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <algorithm>
#include <iomanip>
#include <sstream>

#include "eckit/exception/Exceptions.h"
#include "eckit/utils/MD5.h"
#include "eckit/utils/Translator.h"

#include "fdb5/daos/DaosContainer.h"
#include "fdb5/daos/DaosOID.h"
#include "fdb5/daos/DaosSession.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

void DaosOID::parseReservedBits() {

    otype_ = static_cast<enum daos_otype_t>((oid_.hi & OID_FMT_TYPE_MASK) >> OID_FMT_TYPE_SHIFT);
    oclass_ = OBJ_CLASS_DEF(OR_RP_1, (oid_.hi & OID_FMT_CLASS_MASK) >> OID_FMT_CLASS_SHIFT);
}

DaosOID::DaosOID(const uint64_t& hi, const uint64_t& lo) : oid_({lo, hi}), wasGenerated_(true) {

    parseReservedBits();
}

DaosOID::DaosOID(const std::string& s) : wasGenerated_(true) {

    ASSERT(s.length() == 32);
    ASSERT(std::all_of(s.begin(), s.end(), ::isxdigit));

    oid_.hi = std::stoull(s.substr(0, 16), nullptr, 16);
    oid_.lo = std::stoull(s.substr(16, 16), nullptr, 16);

    parseReservedBits();
}

DaosOID::DaosOID(const uint32_t& hi, const uint64_t& lo, const enum daos_otype_t& otype,
                 const daos_oclass_id_t& oclass) :
    otype_(otype), oid_({lo, hi}), oclass_(oclass), wasGenerated_(false) {}

DaosOID::DaosOID(const std::string& name, const enum daos_otype_t& otype, const daos_oclass_id_t& oclass) :
    otype_(otype), oclass_(oclass), wasGenerated_(false) {

    eckit::MD5 md5(name);
    /// @todo: calculate digests of 12 bytes (24 hex characters) rather than 16 bytes (32 hex characters)
    ///        I have tried redefining MD5_DIGEST_LENGTH but it had no effect
    oid_.hi = std::stoull(md5.digest().substr(0, 8), nullptr, 16);
    oid_.lo = std::stoull(md5.digest().substr(8, 16), nullptr, 16);
}

bool DaosOID::operator==(const DaosOID& rhs) const {

    return oid_.hi == rhs.oid_.hi && oid_.lo == rhs.oid_.lo && oclass_ == rhs.oclass_ &&
           wasGenerated_ == rhs.wasGenerated_;
}

void DaosOID::generateReservedBits(fdb5::DaosContainer& cont) {

    if (wasGenerated_) {
        return;
    }

    DAOS_CALL(daos_obj_generate_oid(cont.getOpenHandle(), &oid_, otype(), oclass(), 0, 0));

    wasGenerated_ = true;

    return;
}

std::string DaosOID::asString() const {

    if (as_string_.has_value()) {
        return as_string_.value();
    }

    ASSERT(wasGenerated_);

    std::stringstream os;
    os << std::setw(16) << std::setfill('0') << std::hex << oid_.hi;
    os << std::setw(16) << std::setfill('0') << std::hex << oid_.lo;
    as_string_.emplace(os.str());
    return as_string_.value();
}

daos_obj_id_t& DaosOID::asDaosObjIdT() {

    return oid_;
}

enum daos_otype_t DaosOID::otype() const {

    return otype_;
}

daos_oclass_id_t DaosOID::oclass() const {

    return oclass_;
}

DaosArrayOID::DaosArrayOID(const uint64_t& hi, const uint64_t& lo) : DaosOID(hi, lo) {
    ASSERT(otype_ == DAOS_OT_ARRAY);
}

DaosArrayOID::DaosArrayOID(const std::string& oid) : DaosOID(oid) {
    ASSERT(otype_ == DAOS_OT_ARRAY);
}

DaosArrayOID::DaosArrayOID(const uint32_t& hi, const uint64_t& lo, const daos_oclass_id_t& oclass) :
    DaosOID(hi, lo, DAOS_OT_ARRAY, oclass) {}

DaosArrayOID::DaosArrayOID(const std::string& name, const daos_oclass_id_t& oclass) :
    DaosOID(name, DAOS_OT_ARRAY, oclass) {}

DaosByteArrayOID::DaosByteArrayOID(const uint64_t& hi, const uint64_t& lo) : DaosOID(hi, lo) {
    ASSERT(otype_ == DAOS_OT_ARRAY_BYTE);
}

DaosByteArrayOID::DaosByteArrayOID(const std::string& oid) : DaosOID(oid) {
    ASSERT(otype_ == DAOS_OT_ARRAY_BYTE);
}

DaosByteArrayOID::DaosByteArrayOID(const uint32_t& hi, const uint64_t& lo, const daos_oclass_id_t& oclass) :
    DaosOID(hi, lo, DAOS_OT_ARRAY_BYTE, oclass) {}

DaosByteArrayOID::DaosByteArrayOID(const std::string& name, const daos_oclass_id_t& oclass) :
    DaosOID(name, DAOS_OT_ARRAY_BYTE, oclass) {}

DaosKeyValueOID::DaosKeyValueOID(const uint64_t& hi, const uint64_t& lo) : DaosOID(hi, lo) {
    ASSERT(otype_ == DAOS_OT_KV_HASHED);
}

DaosKeyValueOID::DaosKeyValueOID(const std::string& oid) : DaosOID(oid) {
    ASSERT(otype_ == DAOS_OT_KV_HASHED);
}

DaosKeyValueOID::DaosKeyValueOID(const uint32_t& hi, const uint64_t& lo, const daos_oclass_id_t& oclass) :
    DaosOID(hi, lo, DAOS_OT_KV_HASHED, oclass) {}

DaosKeyValueOID::DaosKeyValueOID(const std::string& name, const daos_oclass_id_t& oclass) :
    DaosOID(name, DAOS_OT_KV_HASHED, oclass) {}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
