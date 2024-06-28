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
/// @date June 2024

#pragma once

#include "fdb5/database/FieldLocation.h"

#include "eckit/io/rados/RadosKeyValue.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// @note: used in fdb-list index visiting, in DaosIndex::entries. During 
///   visitation, DaosFieldLocations are built, which normally require 
///   retrieving the location information from DAOS, inflicting RPCs.
///   This DaosLazyFieldLocation, instead, remains empty and the actual 
///   information is only be retrieved from DAOS when stableLocation()
///   is called. This allows the visiting mechanism to discard unmatching
///   FieldLocations before any RPC is performed for them. 
class RadosLazyFieldLocation : public FieldLocation {
public:

    RadosLazyFieldLocation(const fdb5::RadosLazyFieldLocation& rhs);
    RadosLazyFieldLocation(const eckit::RadosKeyValue& index, const std::string& key);

    eckit::DataHandle* dataHandle() const override;

    virtual std::shared_ptr<FieldLocation> make_shared() const override;

    virtual void visit(FieldLocationVisitor& visitor) const override;

    virtual std::shared_ptr<FieldLocation> stableLocation() const override;

private: // methods

    std::unique_ptr<fdb5::FieldLocation>& realise() const;

    void print(std::ostream &out) const override;

private: // members

    eckit::RadosKeyValue index_;
    std::string key_;
    mutable std::unique_ptr<fdb5::FieldLocation> fl_;

};


//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
