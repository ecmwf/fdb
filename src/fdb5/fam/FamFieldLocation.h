/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the Horizon Europe programme funded project OpenCUBE
 * (Grant agreement: 101092984) horizon-opencube.eu
 */

/// @file   FamFieldLocation.h
/// @author Metin Cakircali
/// @date   Jun 2024

#include <memory>

#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"

#include "fdb5/database/FieldLocation.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class FamFieldLocation : public FieldLocation {
public:

    FamFieldLocation(const eckit::URI& uri);

    FamFieldLocation(const eckit::URI& uri, const eckit::Offset& offset, const eckit::Length& length,
                     const Key& remapKey);

    FamFieldLocation(eckit::Stream& stream);

    FamFieldLocation(const FamFieldLocation& rhs);

    eckit::DataHandle* dataHandle() const override;

    std::shared_ptr<const FieldLocation> make_shared() const override;

    void visit(FieldLocationVisitor& visitor) const override;

public:  // For Streamable

    static const eckit::ClassSpec& classSpec() { return classSpec_; }

protected:  // For Streamable

    virtual const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }

    virtual void encode(eckit::Stream&) const override;

    static eckit::ClassSpec classSpec_;
    static eckit::Reanimator<FamFieldLocation> reanimator_;

private:  // methods

    void print(std::ostream& out) const override;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
