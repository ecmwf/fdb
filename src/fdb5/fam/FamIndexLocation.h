// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/*
 * This software was developed as part of the Horizon Europe programme funded project OpenCUBE
 * (Grant agreement: 101092984) horizon-opencube.eu
 */

/// @file   FamIndexLocation.h
/// @author Metin Cakircali
/// @date   Mar 2026

#pragma once

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"

#include "fdb5/database/IndexLocation.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// IndexLocation implementation for the FAM catalogue backend.
///
/// Stores the URI of the FamMap table object that backs the index's datum storage.
/// This URI uniquely identifies an index within a FAM region.
class FamIndexLocation : public IndexLocation {

public:  // methods

    explicit FamIndexLocation(eckit::URI uri);

    eckit::URI uri() const override { return uri_; }

    IndexLocation* clone() const override { return new FamIndexLocation(*this); }

protected:  // For Streamable

    void encode(eckit::Stream&) const override { NOTIMP; }

private:  // methods

    void print(std::ostream& out) const override;

private:  // members

    eckit::URI uri_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
