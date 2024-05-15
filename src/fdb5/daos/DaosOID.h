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
/// @date Oct 2022

#pragma once

#include <daos.h>

#include "eckit/utils/Optional.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DaosContainer;

class DaosOID {

public: // methods

    DaosOID(const uint64_t& hi, const uint64_t& lo);
    DaosOID(const std::string&);
    DaosOID(const uint32_t& hi, const uint64_t& lo, const enum daos_otype_t& otype, const daos_oclass_id_t& oclass);
    DaosOID(const std::string&, const enum daos_otype_t& otype, const daos_oclass_id_t& oclass);

    bool operator==(const DaosOID&) const;

    void generateReservedBits(fdb5::DaosContainer&);

    std::string asString() const;
    daos_obj_id_t& asDaosObjIdT();
    enum daos_otype_t otype() const;
    daos_oclass_id_t oclass() const;
    bool wasGenerated() const { return wasGenerated_; };

private: // methods

    void parseReservedBits();

protected: // members

    enum daos_otype_t otype_;

private: // members

    daos_obj_id_t oid_;
    daos_oclass_id_t oclass_;
    bool wasGenerated_;
    mutable eckit::Optional<std::string> as_string_;

};

class DaosArrayOID : public DaosOID {

public: //methods

    DaosArrayOID(const uint64_t& hi, const uint64_t& lo);
    DaosArrayOID(const std::string&);
    DaosArrayOID(const uint32_t& hi, const uint64_t& lo, const daos_oclass_id_t& oclass);
    DaosArrayOID(const std::string&, const daos_oclass_id_t& oclass);

};

class DaosByteArrayOID : public DaosOID {

public: //methods

    DaosByteArrayOID(const uint64_t& hi, const uint64_t& lo);
    DaosByteArrayOID(const std::string&);
    DaosByteArrayOID(const uint32_t& hi, const uint64_t& lo, const daos_oclass_id_t& oclass);
    DaosByteArrayOID(const std::string&, const daos_oclass_id_t& oclass);

};

class DaosKeyValueOID : public DaosOID {

public: //methods

    DaosKeyValueOID(const uint64_t& hi, const uint64_t& lo);
    DaosKeyValueOID(const std::string&);
    DaosKeyValueOID(const uint32_t& hi, const uint64_t& lo, const daos_oclass_id_t& oclass);
    DaosKeyValueOID(const std::string&, const daos_oclass_id_t& oclass);

};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5