// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Nicolau Manubens
/// @date Nov 2022

#pragma once

#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"

#include "fdb5/database/FieldLocation.h"

#include "fdb5/daos/DaosName.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DaosFieldLocation : public FieldLocation {
public:

    DaosFieldLocation(const DaosFieldLocation& rhs);
    DaosFieldLocation(const eckit::URI& uri);
    DaosFieldLocation(const eckit::URI& uri, eckit::Offset offset, eckit::Length length, const Key& remapKey);
    DaosFieldLocation(eckit::Stream&);

    eckit::DataHandle* dataHandle() const override;

    std::shared_ptr<const FieldLocation> make_shared() const override;

    void visit(FieldLocationVisitor& visitor) const override;

public:  // For Streamable

    static const eckit::ClassSpec& classSpec() { return classSpec_; }

protected:  // For Streamable

    const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }

    static eckit::ClassSpec classSpec_;
    static eckit::Reanimator<DaosFieldLocation> reanimator_;

private:  // methods

    void print(std::ostream& out) const override;
};


//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
