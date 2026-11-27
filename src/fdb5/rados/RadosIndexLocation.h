// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Nicolau Manubens
/// @author Metin Cakircali
/// @date Jun 2024

#pragma once

#include "fdb5/database/IndexLocation.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/io/rados/RadosKeyValue.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class RadosIndexLocation : public IndexLocation {

public:  // methods

    explicit RadosIndexLocation(const eckit::RadosKeyValue& name);

    const eckit::RadosKeyValue& radosName() const { return name_; };

    eckit::URI uri() const override { return name_.uri(); }

    IndexLocation* clone() const override { NOTIMP; }

protected:  // For Streamable

    void encode(eckit::Stream&) const override { NOTIMP; }

private:  // methods

    void print(std::ostream& out) const override;

private:  // members

    eckit::RadosKeyValue name_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
