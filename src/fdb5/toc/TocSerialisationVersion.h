/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   TocSerialisationVersion.h
/// @author Emanuele Danovaro
/// @date   Jan 2022

#pragma once

#include "fdb5/config/Config.h"
#include "fdb5/database/SerialisationVersion.h"

namespace fdb5 {

/// Version 2: TOC format originally used in first public release
/// Version 3: TOC serialisation format includes Stream objects
class TocSerialisationVersion : public SerialisationVersion {

public:
    TocSerialisationVersion(const fdb5::Config& config);
    virtual ~TocSerialisationVersion();

    /// Defines the serialisation versions the software is able to handle
    /// Entries with these versions will not issue errors
    std::vector<unsigned int> supported() const override;

    /// Latest version of serialisation the software is capable to create
    /// To be used as default
    unsigned int latest() const override;

    /// Default version of serialisation the software will use
    /// This may be behind the latest version
    unsigned int defaulted() const override;

};

} // namespace fdb5
