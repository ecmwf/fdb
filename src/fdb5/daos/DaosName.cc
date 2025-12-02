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
#include "eckit/filesystem/PathName.h"
#include "eckit/utils/Tokenizer.h"

#include "fdb5/daos/DaosArrayHandle.h"
#include "fdb5/daos/DaosArrayPartHandle.h"
#include "fdb5/daos/DaosContainer.h"
#include "fdb5/daos/DaosException.h"
#include "fdb5/daos/DaosKeyValueHandle.h"
#include "fdb5/daos/DaosName.h"
#include "fdb5/daos/DaosPool.h"
#include "fdb5/daos/DaosSession.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosNameBase::DaosNameBase(const std::string& pool) : pool_(pool) {}

DaosNameBase::DaosNameBase(const std::string& pool, const std::string& cont) : pool_(pool), cont_(cont) {}

DaosNameBase::DaosNameBase(const std::string& pool, const std::string& cont, const fdb5::DaosOID& oid) :
    pool_(pool), cont_(cont), oid_(oid) {}

DaosNameBase::DaosNameBase(const eckit::URI& uri) {

    /// @note: uri expects a string with the following format:
    ///   daos:pool[/cont[/oidhi.oidlo]]

    ASSERT(uri.scheme() == "daos");
    ASSERT(uri.query() == std::string());
    ASSERT(uri.fragment() == std::string());

    eckit::Tokenizer parse("/");
    std::vector<std::string> bits;
    parse(uri.name(), bits);

    ASSERT(bits.size() > 0);
    ASSERT(bits.size() < 4);

    pool_ = bits[0];
    if (bits.size() > 1) {
        cont_.emplace(bits[1]);
    }
    if (bits.size() > 2) {
        oid_.emplace(bits[2]);
    }
}

DaosNameBase::DaosNameBase(const fdb5::DaosObject& obj) :
    pool_(obj.getContainer().getPool().name()), cont_(obj.getContainer().name()), oid_(obj.OID()) {}

fdb5::DaosOID DaosNameBase::allocOID(const daos_otype_t& otype, const daos_oclass_id_t& oclass) const {

    ASSERT(cont_.has_value());
    ASSERT(!oid_.has_value());

    fdb5::DaosSession s{};
    fdb5::DaosPool& p = s.getPool(pool_);
    fdb5::DaosContainer& c = p.getContainer(cont_.value());

    return fdb5::DaosOID{0, c.allocateOIDLo(), otype, oclass};
}

/// @note: to be called in all DaosName* methods and DaosName* user code that intend
///   having this Name instance's OID generated (having its reserved bits populated)
///   as a post-condition.
void DaosNameBase::ensureGeneratedOID() const {

    ASSERT(cont_.has_value());
    ASSERT(oid_.has_value());

    if (oid_.value().wasGenerated()) {
        return;
    }

    fdb5::DaosSession s{};
    fdb5::DaosPool& p = s.getPool(pool_);
    fdb5::DaosContainer& c = p.getContainer(cont_.value());

    oid_.value().generateReservedBits(c);
}

/// @todo: the otype parameter is unnecessary
std::unique_ptr<fdb5::DaosObject> DaosNameBase::buildObject(fdb5::DaosSession& s) const {

    /// @note: will throw DaosEntityNotFoundException if not exists
    if (oid_->otype() == DAOS_OT_ARRAY || oid_->otype() == DAOS_OT_ARRAY_BYTE) {
        return std::make_unique<fdb5::DaosArray>(s, *this);
    }
    if (oid_->otype() == DAOS_OT_KV_HASHED) {
        return std::make_unique<fdb5::DaosKeyValue>(s, *this);
    }
    throw eckit::Exception("Provided otype not recognised.");
}

void DaosNameBase::create() const {

    ASSERT(cont_.has_value());

    fdb5::DaosSession s{};
    fdb5::DaosPool& p = s.getPool(pool_);

    fdb5::DaosContainer& c = p.ensureContainer(cont_.value());

    if (!oid_.has_value()) {
        return;
    }

    ensureGeneratedOID();

    switch (oid_.value().otype()) {
        case DAOS_OT_ARRAY:
        case DAOS_OT_ARRAY_BYTE:
            c.createArray(oid_.value());
            break;
        case DAOS_OT_KV_HASHED:
            c.createKeyValue(oid_.value());
            break;
        default:
            throw eckit::Exception("Provided otype not recognised.");
    }
}

void DaosNameBase::destroy() const {

    ASSERT(cont_.has_value());

    fdb5::DaosSession s{};
    fdb5::DaosPool& p = s.getPool(pool_);

    /// @todo: are the semantics here OK? i.e. not throw if cont or obj not found

    if (!oid_.has_value()) {
        try {
            p.destroyContainer(cont_.value());
        }
        catch (fdb5::DaosEntityNotFoundException& e) {
        }
        return;
    }

    fdb5::DaosContainer& c = p.getContainer(cont_.value());

    ensureGeneratedOID();

    /// @todo: improve this code

    if (oid_->otype() == DAOS_OT_KV_HASHED) {

        try {
            fdb5::DaosKeyValue kv{c, oid_.value()};
            kv.destroy();
        }
        catch (fdb5::DaosEntityNotFoundException& e) {
        }
    }
    else if (oid_->otype() == DAOS_OT_ARRAY || oid_->otype() == DAOS_OT_ARRAY_BYTE) {

        try {
            fdb5::DaosArray a{c, oid_.value()};
            a.destroy();
        }
        catch (fdb5::DaosEntityNotFoundException& e) {
        }
    }
    else {

        NOTIMP;
    }
}

eckit::Length DaosNameBase::size() const {

    if (!oid_.has_value()) {
        NOTIMP;
    }

    ensureGeneratedOID();

    fdb5::DaosSession s{};
    return eckit::Length(buildObject(s)->size());
}

bool DaosNameBase::exists() const {

    fdb5::DaosSession s{};

    try {

        fdb5::DaosPool& p = s.getPool(pool_);

        if (cont_.has_value()) {
            fdb5::DaosContainer& c = p.getContainer(cont_.value());
        }

        if (oid_.has_value()) {
            ensureGeneratedOID();
            buildObject(s);
        }
    }
    catch (const fdb5::DaosEntityNotFoundException& e) {
        return false;
    }
    return true;
}

std::string DaosNameBase::asString() const {

    if (as_string_.has_value()) {
        return as_string_.value();
    }

    if (oid_.has_value()) {
        ensureGeneratedOID();
    }

    eckit::StringList v{pool_};
    if (cont_.has_value()) {
        v.push_back(cont_.value());
    }
    if (oid_.has_value()) {
        v.push_back(oid_.value().asString());
    }

    std::ostringstream oss;
    const char* sep = "";
    for (eckit::StringList::const_iterator j = v.begin(); j != v.end(); ++j) {
        oss << sep;
        oss << *j;
        sep = "/";
    }

    as_string_.emplace(oss.str());

    return as_string_.value();
}

eckit::URI DaosNameBase::URI() const {

    return eckit::URI("daos", eckit::PathName(asString()));
}

const std::string& DaosNameBase::poolName() const {

    return pool_;
}

const std::string& DaosNameBase::containerName() const {

    ASSERT(cont_.has_value());
    return cont_.value();
}

const fdb5::DaosOID& DaosNameBase::OID() const {

    ASSERT(oid_.has_value());
    ensureGeneratedOID();
    return oid_.value();
}

bool DaosNameBase::operator<(const DaosNameBase& other) const {
    return this->asString() < other.asString();
}

bool DaosNameBase::operator>(const DaosNameBase& other) const {
    return this->asString() > other.asString();
}

bool DaosNameBase::operator!=(const DaosNameBase& other) const {
    return this->asString() != other.asString();
}

bool DaosNameBase::operator==(const DaosNameBase& other) const {
    return this->asString() == other.asString();
}

bool DaosNameBase::operator<=(const DaosNameBase& other) const {
    return this->asString() <= other.asString();
}

bool DaosNameBase::operator>=(const DaosNameBase& other) const {
    return this->asString() >= other.asString();
}

DaosArrayName::DaosArrayName(const std::string& pool, const std::string& cont, const fdb5::DaosOID& oid) :
    DaosNameBase(pool, cont, oid) {

    ASSERT(oid_.value().otype() == DAOS_OT_ARRAY || oid_.value().otype() == DAOS_OT_ARRAY_BYTE);
}

DaosArrayName::DaosArrayName(const eckit::URI& uri) : DaosNameBase(uri) {

    ASSERT(oid_.has_value());
    ASSERT(oid_.value().otype() == DAOS_OT_ARRAY || oid_.value().otype() == DAOS_OT_ARRAY_BYTE);
}

DaosArrayName::DaosArrayName(const fdb5::DaosArray& arr) : DaosNameBase(arr) {}

eckit::DataHandle* DaosArrayName::dataHandle(bool overwrite) const {

    return new fdb5::DaosArrayHandle(*this);
}

eckit::DataHandle* DaosArrayName::dataHandle(const eckit::Offset& off, const eckit::Length& len) const {

    return new fdb5::DaosArrayPartHandle(*this, off, len);
}

DaosKeyValueName::DaosKeyValueName(const std::string& pool, const std::string& cont, const fdb5::DaosOID& oid) :
    DaosNameBase(pool, cont, oid) {

    ASSERT(oid_.value().otype() == DAOS_OT_KV_HASHED);
}

DaosKeyValueName::DaosKeyValueName(const eckit::URI& uri) : DaosNameBase(uri) {

    ASSERT(oid_.has_value());
    ASSERT(oid_.value().otype() == DAOS_OT_KV_HASHED);
}

DaosKeyValueName::DaosKeyValueName(const fdb5::DaosKeyValue& kv) : DaosNameBase(kv) {}

bool DaosKeyValueName::has(const std::string& key) const {

    ASSERT(exists());

    fdb5::DaosSession s{};
    fdb5::DaosKeyValue kv{s, *this};

    return kv.has(key);
}

eckit::DataHandle* DaosKeyValueName::dataHandle(const std::string& key, bool overwrite) const {

    /// @todo: OK to serialise pointers in DaosName?
    return new fdb5::DaosKeyValueHandle(*this, key);
}

DaosName::DaosName(const std::string& pool) : DaosNameBase(pool) {}

DaosName::DaosName(const std::string& pool, const std::string& cont) : DaosNameBase(pool, cont) {}

DaosName::DaosName(const std::string& pool, const std::string& cont, const fdb5::DaosOID& oid) :
    DaosNameBase(pool, cont, oid) {}

DaosName::DaosName(const eckit::URI& uri) : DaosNameBase(uri) {}

DaosName::DaosName(const fdb5::DaosObject& obj) : DaosNameBase(obj) {}

DaosArrayName DaosName::createArrayName(const daos_oclass_id_t& oclass, bool with_attr) const {

    ASSERT(hasContainerName() && !hasOID());
    create();
    if (with_attr) {
        return DaosArrayName{pool_, cont_.value(), allocOID(DAOS_OT_ARRAY, oclass)};
    }
    else {
        return DaosArrayName{pool_, cont_.value(), allocOID(DAOS_OT_ARRAY_BYTE, oclass)};
    }
}

DaosKeyValueName DaosName::createKeyValueName(const daos_oclass_id_t& oclass) const {

    ASSERT(hasContainerName() && !hasOID());
    create();
    return DaosKeyValueName{pool_, cont_.value(), allocOID(DAOS_OT_KV_HASHED, oclass)};
}

std::vector<fdb5::DaosOID> DaosName::listOIDs() const {

    ASSERT(hasContainerName() && !hasOID());

    fdb5::DaosSession s{};
    fdb5::DaosPool& p = s.getPool(pool_);
    fdb5::DaosContainer& c = p.getContainer(cont_.value());

    return c.listOIDs();
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
