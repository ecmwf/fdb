// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   TocSerialisationVersion.h
/// @author Emanuele Danovaro
/// @date   Jan 2022

#pragma once

#include "fdb5/config/Config.h"

namespace fdb5 {

/// Version 2: TOC format originally used in first public release
/// Version 3: TOC serialisation format includes Stream objects
class TocSerialisationVersion {

public:

    TocSerialisationVersion(const fdb5::Config& config);
    virtual ~TocSerialisationVersion();

    /// Defines the serialisation versions the software is able to handle
    /// Entries with these versions will not issue errors
    static std::vector<unsigned int> supported();

    /// Latest version of serialisation the software is capable to create
    /// This may not be the default
    static unsigned int latest();

    /// Default version of serialisation the software will use
    /// This may be behind the latest version
    static unsigned int defaulted();

    /// List of supported versions as a string
    static std::string supportedStr();

    /// Defines the serialisation version the software is meant to create new entries with
    /// Normally is the latest()
    /// But can be overriden by the variable FDB5_SERIALISATION_VERSION
    unsigned int used() const;

    /// Checks the serialisation version is supported by the software
    bool check(unsigned int version, bool throwOnFail = true) const;

private:  // members

    unsigned int used_;  //< version to be used for serialisation on write
};

}  // namespace fdb5
