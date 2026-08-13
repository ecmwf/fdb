/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Emanuele Danovaro
/// @author Nicolau Manubens
/// @date Feb 2024

#pragma once

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
