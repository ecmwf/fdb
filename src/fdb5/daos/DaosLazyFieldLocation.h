// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Nicolau Manubens
/// @date Oct 2023

#pragma once

#include "fdb5/database/FieldLocation.h"

#include "fdb5/daos/DaosName.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// @note: used in fdb-list index visiting, in DaosIndex::entries. During
///   visitation, DaosFieldLocations are built, which normally require
///   retrieving the location information from DAOS, inflicting RPCs.
///   This DaosLazyFieldLocation, instead, remains empty and the actual
///   information is only be retrieved from DAOS when stableLocation()
///   is called. This allows the visiting mechanism to discard unmatching
///   FieldLocations before any RPC is performed for them.
class DaosLazyFieldLocation : public FieldLocation {
public:

    DaosLazyFieldLocation(const fdb5::DaosLazyFieldLocation& rhs);
    DaosLazyFieldLocation(const fdb5::DaosKeyValueName& index, const std::string& key);

    eckit::DataHandle* dataHandle() const override;

    std::shared_ptr<const FieldLocation> make_shared() const override;

    void visit(FieldLocationVisitor& visitor) const override;

    std::shared_ptr<const FieldLocation> stableLocation() const override;

private:  // methods

    std::unique_ptr<fdb5::FieldLocation>& realise() const;

    void print(std::ostream& out) const override;

private:  // members

    fdb5::DaosKeyValueName index_;
    std::string key_;
    mutable std::unique_ptr<fdb5::FieldLocation> fl_;
};


//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
