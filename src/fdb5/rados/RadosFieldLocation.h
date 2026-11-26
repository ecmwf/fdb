// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Emanuele Danovaro
/// @author Nicolau Manubens
/// @author Metin Cakircali
/// @date Feb 2024

#pragma once

#include "eckit/filesystem/PathName.h"
#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"

#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Key.h"

#include "eckit/filesystem/URI.h"
#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"
#include "eckit/serialisation/Reanimator.h"

#include <memory>
#include <ostream>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class RadosFieldLocation : public FieldLocation {
public:

    RadosFieldLocation(const RadosFieldLocation& rhs);
    RadosFieldLocation(const eckit::URI& uri);
    RadosFieldLocation(const eckit::URI& uri, eckit::Offset offset, eckit::Length length);
    // Factory-only overload; `remapKey` is ignored (see RadosFieldLocation.cc).
    RadosFieldLocation(const eckit::URI& uri, eckit::Offset offset, eckit::Length length, const Key& remapKey);
    RadosFieldLocation(eckit::Stream&);

    eckit::DataHandle* dataHandle() const override;

    std::shared_ptr<const FieldLocation> make_shared() const override;

    void visit(FieldLocationVisitor& visitor) const override;

public:  // For Streamable

    static const eckit::ClassSpec& classSpec() { return classSpec_; }

protected:  // For Streamable

    const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }

    static eckit::ClassSpec classSpec_;
    static eckit::Reanimator<RadosFieldLocation> reanimator_;

private:  // methods

    void print(std::ostream& out) const override;
};


//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
