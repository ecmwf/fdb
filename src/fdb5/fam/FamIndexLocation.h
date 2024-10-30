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

/// @file   FamIndexLocation.h
/// @author Metin Cakircali
/// @date   Jul 2024

#pragma once

#include "eckit/io/fam/FamObjectName.h"
#include "eckit/serialisation/Reanimator.h"
#include "fdb5/database/IndexLocation.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class FamIndexLocation: public IndexLocation {
public:  // methods
    FamIndexLocation(const eckit::FamObjectName& object);

    FamIndexLocation(eckit::Stream& stream);

    auto uri() const -> eckit::URI override;

    auto clone() const -> IndexLocation* override;

public:  // Streamable
    static auto classSpec() -> const eckit::ClassSpec& { return classSpec_; }

protected:  // Streamable
    static eckit::ClassSpec classSpec_;

    static eckit::Reanimator<FamIndexLocation> reanimator_;

    auto reanimator() const -> const eckit::ReanimatorBase& override { return reanimator_; }

    void encode(eckit::Stream& stream) const override;

private:  // methods
    void print(std::ostream& out) const override;

private:  // members
    eckit::FamObjectName object_;

private:  // friends
    friend class FamIndex;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
