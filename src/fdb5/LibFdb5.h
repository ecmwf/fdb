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

#include "fdb5/api/helpers/Callback.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/types/TypesRegistry.h"

namespace fdb5 {

class Config;

//----------------------------------------------------------------------------------------------------------------------

class RemoteProtocolVersion {
public:

    RemoteProtocolVersion();

    /// Defines the serialisation versions the software is able to handle
    /// Entries with these versions will not issue errors
    std::vector<unsigned int> supported() const;

    /// List of supported versions as a string
    std::string supportedStr() const;

    /// Latest version of serialisation the software is capable to create
    /// To be used as default
    unsigned int latest() const;

    /// Default version of serialisation the software will use
    /// This may be behind the latest version
    unsigned int defaulted() const;

    /// Defines the serialisation version the software is meant to create new entries with
    /// Normally is the latest()
    /// But can be overriden by the variable FDB5_REMOTE_PROTOCOL_VERSION
    unsigned int used() const;

    /// Checks the serialisation version is supported by the software
    bool check(unsigned int version, bool throwOnFail = true);

private:

    unsigned int used_;  //< version to be used for remote protocol
};

//----------------------------------------------------------------------------------------------------------------------

class LibFdb5 : public eckit::system::Library {
public:

    LibFdb5();

    static LibFdb5& instance();

    /// Returns an object describing the remote protocol version
    RemoteProtocolVersion remoteProtocolVersion() const;

    /// Returns the default configuration according to the rules of FDB configuration search
    const Config& defaultConfig(const eckit::Configuration& userConfig = eckit::LocalConfiguration());

    bool dontDeregisterFactories() const;

    void registerConstructorCallback(ConstructorCallback cb);

    ConstructorCallback constructorCallback();

    static const std::set<std::string>& auxiliaryRegistry();

protected:

    virtual std::string version() const;

    virtual std::string gitsha1(unsigned int count) const;

private:

    std::unique_ptr<Config> config_;
    ConstructorCallback constructorCallback_ = CALLBACK_CONSTRUCTOR_NOOP;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
