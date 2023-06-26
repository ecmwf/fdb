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
/// @date Sept 2022

#pragma once

#include <daos.h>

#include <string>
#include <memory>

#include "eckit/utils/Optional.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/DataHandle.h"

#include "fdb5/daos/DaosObject.h"
#include "fdb5/daos/DaosOID.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DaosSession;
class DaosPool;
class DaosContainer;

class DaosNameBase {

public: // methods

    // TODO: implement DaosName::contName to return a container DaosName, and rename poolName and contName to something else
    // DaosName poolName() const;
    // DaosName contName() const;

    void generateOID() const;

    void create() const;
    void destroy() const;
    eckit::Length size() const;
    bool exists() const;
    // owner
    // empty

    // TODO: asString should only be used for debugging. private print?
    std::string asString() const;
    eckit::URI URI() const;
    std::string poolName() const;
    std::string contName() const;
    bool hasContName() const { return cont_.has_value(); };
    fdb5::DaosOID OID() const;
    bool hasOID() const { return oid_.has_value(); };

    bool operator<(const DaosNameBase& other) const;
    bool operator>(const DaosNameBase& other) const;
    bool operator!=(const DaosNameBase& other) const;
    bool operator==(const DaosNameBase& other) const;
    bool operator<=(const DaosNameBase& other) const;
    bool operator>=(const DaosNameBase& other) const;

protected: // methods

    DaosNameBase(const std::string& pool);
    DaosNameBase(const std::string& pool, const std::string& cont);
    DaosNameBase(const std::string& pool, const std::string& cont, const fdb5::DaosOID& oid);
    DaosNameBase(const eckit::URI&);
    DaosNameBase(const fdb5::DaosObject&);

    fdb5::DaosOID createOID(const daos_otype_t&, const daos_oclass_id_t&) const;

private: // methods

    std::unique_ptr<fdb5::DaosObject> buildObject(const daos_otype_t& otype, fdb5::DaosSession&) const;

protected: // members

    std::string pool_;
    eckit::Optional<std::string> cont_;
    mutable eckit::Optional<fdb5::DaosOID> oid_;

};

class DaosArrayName : public DaosNameBase {

public: // methods

    DaosArrayName(const std::string& pool, const std::string& cont, const fdb5::DaosOID& oid);
    DaosArrayName(const eckit::URI&);
    DaosArrayName(const fdb5::DaosArray&);

    /// @todo: think if this overwrite parameter makes sense in DAOS
    eckit::DataHandle* dataHandle(bool overwrite = false) const;

};

class DaosKeyValueName : public DaosNameBase {

public: // methods

    DaosKeyValueName(const std::string& pool, const std::string& cont, const fdb5::DaosOID& oid);
    DaosKeyValueName(const eckit::URI&);
    DaosKeyValueName(const fdb5::DaosKeyValue&);

    bool has(const std::string& key) const;

    /// @todo: think if this overwrite parameter makes sense in DAOS
    eckit::DataHandle* dataHandle(const std::string& key, bool overwrite = false) const;

};

/// @note: DaosName can represent a pool, container or object. Provides convenient functionality for containers
class DaosName : public DaosNameBase {

public: // methods

    DaosName(const std::string& pool);
    DaosName(const std::string& pool, const std::string& cont);
    DaosName(const std::string& pool, const std::string& cont, const fdb5::DaosOID& oid);
    DaosName(const eckit::URI&);
    DaosName(const fdb5::DaosObject&);

    fdb5::DaosArrayName createArrayName(const daos_oclass_id_t& oclass = OC_S1) const;
    fdb5::DaosKeyValueName createKeyValueName(const daos_oclass_id_t& oclass = OC_S1) const;
    std::vector<fdb5::DaosOID> listOIDs() const;

};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
