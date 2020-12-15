/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Nov 2016

#pragma once

#include <string>
#include <vector>

#include "eckit/system/Library.h"

#include "fdb5/database/DB.h"
#include "fdb5/types/TypesRegistry.h"

namespace fdb5 {

class Config;

//----------------------------------------------------------------------------------------------------------------------

class LibFdb5 : public eckit::system::Library {
public:

    LibFdb5();

    static LibFdb5& instance();

    /// Latest version of serialisation the software is capable to create
    /// To be used as default
    unsigned int latestSerialisationVersion() const;

    /// Defines the serialisation version the software is meant to create new entries with
    /// Normally is the latestSerialisationVersion()
    /// But can be overriden by the variable FDB5_SERIALISATION_VERSION
    /// Version 2: TOC format originally used in first public release
    /// Version 3: TOC serialisation format includes Stream objects
    unsigned int useSerialisationVersion() const;

    /// Defines the serialisation versions the software is able to handle
    /// Entries with these versions will not issue errors
    std::vector<unsigned int> supportedSerialisationVersions() const;

    Config defaultConfig();

protected:

    const void* addr() const;

    virtual std::string version() const;

    virtual std::string gitsha1(unsigned int count) const;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
