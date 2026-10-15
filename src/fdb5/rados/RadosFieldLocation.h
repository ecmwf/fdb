// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Emanuele Danovaro
/// @date Jan 2020

#pragma once

#include "eckit/filesystem/PathName.h"
#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"

#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/FileStore.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class RadosFieldLocation : public FieldLocation {
public:

    RadosFieldLocation(const RadosFieldLocation& rhs);
    RadosFieldLocation(const eckit::PathName path, eckit::Offset offset, eckit::Length length);
    RadosFieldLocation(const eckit::URI& uri);
    RadosFieldLocation(const eckit::URI& uri, eckit::Offset offset, eckit::Length length);
    RadosFieldLocation(eckit::Stream&);
    eckit::DataHandle* dataHandle() const override;
    eckit::DataHandle* dataHandle(const Key& remapKey) const override;
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
    eckit::URI uri(const eckit::PathName& path);
};


//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
