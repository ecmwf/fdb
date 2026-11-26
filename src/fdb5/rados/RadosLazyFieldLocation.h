// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Nicolau Manubens
/// @author Metin Cakircali
/// @date June 2024

#pragma once

#include "fdb5/database/FieldLocation.h"

#include "eckit/io/rados/RadosKeyValue.h"

#include <memory>
#include <ostream>
#include <string>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

// Used by fdb-list index visiting in RadosIndex::entries. Instances remain empty until the
// visitor accepts the enclosing key; only then does stableLocation() trigger the RADOS read
// and reanimate the concrete RadosFieldLocation. This avoids RPCs for unmatched keys.
class RadosLazyFieldLocation : public FieldLocation {
public:

    RadosLazyFieldLocation(const fdb5::RadosLazyFieldLocation& rhs);
    RadosLazyFieldLocation(const eckit::RadosKeyValue& index, const std::string& key);

    eckit::DataHandle* dataHandle() const override;

    virtual std::shared_ptr<const FieldLocation> make_shared() const override;

    virtual void visit(FieldLocationVisitor& visitor) const override;

    virtual std::shared_ptr<const FieldLocation> stableLocation() const override;

private:  // methods

    std::unique_ptr<fdb5::FieldLocation>& realise() const;

    void print(std::ostream& out) const override;

private:  // members

    eckit::RadosKeyValue index_;
    std::string key_;
    mutable std::unique_ptr<fdb5::FieldLocation> fl_;
};


//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
