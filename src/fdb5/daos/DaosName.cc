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
#include "eckit/utils/Tokenizer.h"
#include "eckit/filesystem/PathName.h"

#include "fdb5/daos/DaosName.h"
#include "fdb5/daos/DaosSession.h"
#include "fdb5/daos/DaosPool.h"
#include "fdb5/daos/DaosContainer.h"
#include "fdb5/daos/DaosArrayHandle.h"
#include "fdb5/daos/DaosArrayPartHandle.h"
#include "fdb5/daos/DaosKeyValueHandle.h"
#include "fdb5/daos/DaosException.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosNameBase::DaosNameBase(const std::string& pool) : pool_(pool) {}

DaosNameBase::DaosNameBase(const std::string& pool, const std::string& cont) : pool_(pool), cont_(cont) {}

DaosNameBase::DaosNameBase(const std::string& pool, const std::string& cont, const fdb5::DaosOID& oid) : pool_(pool), cont_(cont), oid_(oid) {}

DaosNameBase::DaosNameBase(const eckit::URI& uri)  {

    /// @todo: find format for documenting inputs
    // Inputs:
    //   uri: daos:pool[/cont[/oidhi.oidlo]]

    ASSERT(uri.scheme() == "daos");
    ASSERT(uri.query() == std::string());
    ASSERT(uri.fragment() == std::string());

    eckit::Tokenizer parse("/");
    std::vector<std::string> bits;
    parse(uri.name(), bits);

    ASSERT(bits.size() > 0);
    ASSERT(bits.size() < 4);

    pool_ = bits[0];
    if (bits.size() > 1) cont_.emplace(bits[1]);
    if (bits.size() > 2) oid_.emplace(bits[2]);

}

DaosNameBase::DaosNameBase(const fdb5::DaosObject& obj) : 
    pool_(obj.getContainer().getPool().name()), 
    cont_(obj.getContainer().name()), 
    oid_(obj.OID()) {}

fdb5::DaosOID DaosNameBase::createOID(const daos_otype_t& otype, const daos_oclass_id_t& oclass) const {

    ASSERT(cont_.has_value());
    ASSERT(!oid_.has_value());

    fdb5::DaosSession s{};
    fdb5::DaosPool& p = s.getPool(pool_);
    fdb5::DaosContainer& c = p.getContainer(cont_.value());

    return fdb5::DaosOID{0, c.allocateOIDLo(), otype, oclass};

}

void DaosNameBase::generateOID() const {

    ASSERT(cont_.has_value());
    ASSERT(oid_.has_value());

    if (oid_.value().wasGenerated()) return;
    
    fdb5::DaosSession s{};
    fdb5::DaosPool& p = s.getPool(pool_);
    fdb5::DaosContainer& c = p.getContainer(cont_.value());

    oid_.value().generate(c);

}

/// @todo: the otype parameter is unnecessary
std::unique_ptr<fdb5::DaosObject> DaosNameBase::buildObject(const daos_otype_t& otype, fdb5::DaosSession& s) const {

    // will throw DaosEntityNotFoundException if not exists
    if (otype == DAOS_OT_ARRAY || otype == DAOS_OT_ARRAY_BYTE)
        return std::unique_ptr<fdb5::DaosObject>(new fdb5::DaosArray{s, *this});
    if (otype == DAOS_OT_KV_HASHED) return std::unique_ptr<fdb5::DaosObject>(new fdb5::DaosKeyValue{s, *this});
    throw eckit::Exception("Provided otype not recognised.");

}

void DaosNameBase::create() const {

    ASSERT(cont_.has_value());

    fdb5::DaosSession s{};
    fdb5::DaosPool& p = s.getPool(pool_);

    fdb5::DaosContainer& c = p.ensureContainer(cont_.value());
    
    if (!oid_.has_value()) return;

    generateOID();

    switch (oid_.value().otype()) {
        case DAOS_OT_ARRAY: case DAOS_OT_ARRAY_BYTE:
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
        } catch (fdb5::DaosEntityNotFoundException& e) {}
        return;
    }

    fdb5::DaosContainer& c = p.getContainer(cont_.value());

    /// @todo: improve this code

    if (oid_->otype() == DAOS_OT_KV_HASHED) {

        try {
            fdb5::DaosKeyValue kv{c, oid_.value()};
            kv.destroy();
        } catch (fdb5::DaosEntityNotFoundException& e) {}

    } else if (oid_->otype() == DAOS_OT_ARRAY || oid_->otype() == DAOS_OT_ARRAY_BYTE) {

        try {
            fdb5::DaosArray a{c, oid_.value()};
            a.destroy();
        } catch (fdb5::DaosEntityNotFoundException& e) {}

    } else {

        NOTIMP;

    }

}

eckit::Length DaosNameBase::size() const {

    if (!oid_.has_value()) NOTIMP;
    generateOID();

    fdb5::DaosSession s{};
    return eckit::Length(buildObject(oid_.value().otype(), s)->size());

}

bool DaosNameBase::exists() const {

    fdb5::DaosSession s{};

    try {

        fdb5::DaosPool& p = s.getPool(pool_);

        if (cont_.has_value())
            fdb5::DaosContainer& c = p.getContainer(cont_.value());

        if (oid_.has_value()) {
            generateOID();
            buildObject(oid_.value().otype(), s);
        }

    } catch (const fdb5::DaosEntityNotFoundException& e) {
        return false;
    }
    return true;

}

std::string DaosNameBase::asString() const {

    if (oid_.has_value()) generateOID();

    eckit::StringList v{pool_};
    if (cont_.has_value()) v.push_back(cont_.value());
    if (oid_.has_value()) v.push_back(oid_.value().asString());

    std::ostringstream oss;
    const char *sep = "";
    for (eckit::StringList::const_iterator j = v.begin(); j != v.end(); ++j) {
        oss << sep;
        oss << *j;
        sep = "/";
    }

    return oss.str();

}

eckit::URI DaosNameBase::URI() const {

    return eckit::URI("daos", eckit::PathName(asString()));

}

std::string DaosNameBase::poolName() const {

    return pool_;
    
}

// TODO: containerName, make it longer
// actually DaosContainerName::name()
std::string DaosNameBase::contName() const {

    ASSERT(cont_.has_value());
    return cont_.value();
    
}

fdb5::DaosOID DaosNameBase::OID() const {

    ASSERT(oid_.has_value());
    generateOID();
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

DaosArrayName::DaosArrayName(const std::string& pool, const std::string& cont, const fdb5::DaosOID& oid) : DaosNameBase(pool, cont, oid) {
    
    ASSERT(oid_.value().otype() == DAOS_OT_ARRAY || oid_.value().otype() == DAOS_OT_ARRAY_BYTE);
    
}

DaosArrayName::DaosArrayName(const eckit::URI& uri) : DaosNameBase(uri) {

    ASSERT(oid_.has_value());
    ASSERT(oid_.value().otype() == DAOS_OT_ARRAY || oid_.value().otype() == DAOS_OT_ARRAY_BYTE);

}

DaosArrayName::DaosArrayName(const fdb5::DaosArray& arr) : DaosNameBase(arr) {}

eckit::DataHandle* DaosArrayName::dataHandle(bool overwrite) const {

    /// @todo: OK to serialise pointers in DaosName?
    return new fdb5::DaosArrayHandle(*this);

}

eckit::DataHandle* DaosArrayName::dataHandle(const eckit::Offset& off, const eckit::Length& len) const {

    /// @todo: OK to serialise pointers in DaosName?
    return new fdb5::DaosArrayPartHandle(*this, off, len);

}

DaosKeyValueName::DaosKeyValueName(const std::string& pool, const std::string& cont, const fdb5::DaosOID& oid) : DaosNameBase(pool, cont, oid) {
    
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

DaosName::DaosName(const std::string& pool, const std::string& cont, const fdb5::DaosOID& oid) : DaosNameBase(pool, cont, oid) {}

DaosName::DaosName(const eckit::URI& uri) : DaosNameBase(uri) {}

DaosName::DaosName(const fdb5::DaosObject& obj) : DaosNameBase(obj) {}

DaosArrayName DaosName::createArrayName(const daos_oclass_id_t& oclass, bool with_attr) const {

    ASSERT(hasContName() && !hasOID());
    create();
    if (with_attr) {
        return DaosArrayName{pool_, cont_.value(), createOID(DAOS_OT_ARRAY, oclass)};
    } else {
        return DaosArrayName{pool_, cont_.value(), createOID(DAOS_OT_ARRAY_BYTE, oclass)};
    }

}

DaosKeyValueName DaosName::createKeyValueName(const daos_oclass_id_t& oclass) const {

    ASSERT(hasContName() && !hasOID());
    create();
    return DaosKeyValueName{pool_, cont_.value(), createOID(DAOS_OT_KV_HASHED, oclass)};

}

std::vector<fdb5::DaosOID> DaosName::listOIDs() const {

    ASSERT(hasContName() && !hasOID());

    fdb5::DaosSession s{};
    fdb5::DaosPool& p = s.getPool(pool_);
    fdb5::DaosContainer& c = p.getContainer(cont_.value());

    return c.listOIDs();

}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
