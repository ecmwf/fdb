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

#include <vector>

namespace fdb5 {

/// Version 2: TOC format originally used in first public release
/// Version 3: TOC serialisation format includes Stream objects
/// Version 4: TOC serialisation format includes Stream objects without name
class TocSerialisationVersion {
public:  // types
    using version_type = unsigned int;

public:  // methods
    TocSerialisationVersion(const fdb5::Config& config);

    virtual ~TocSerialisationVersion() = default;

    /// Defines the serialisation versions the software is able to handle
    /// Entries with these versions will not issue errors
    static std::vector<version_type> supported();

    /// Latest version of serialisation the software is capable to create
    /// To be used as default
    static version_type latest();

    /// Default version of serialisation the software will use
    /// This may be behind the latest version
    static version_type defaulted();

    /// List of supported versions as a string
    static std::string supportedStr();

    /// Defines the serialisation version the software is meant to create new entries with
    /// Normally is the latest()
    /// But can be overriden by the variable FDB5_SERIALISATION_VERSION
    version_type used() const;

    /// Checks the serialisation version is supported by the software
    static bool check(version_type version, bool throwOnFail = true);

private: // members
    version_type used_;  //< version to be used for serialisation on write
};

} // namespace fdb5
