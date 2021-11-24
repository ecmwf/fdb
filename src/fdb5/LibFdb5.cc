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

#include <algorithm>

#include "fdb5/LibFdb5.h"

#include "eckit/config/LibEcKit.h"
#include "eckit/config/Resource.h"
#include "eckit/config/YAMLConfiguration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/log/Log.h"

#include "fdb5/fdb5_version.h"

#include "fdb5/config/Config.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

REGISTER_LIBRARY(LibFdb5);

LibFdb5::LibFdb5() : Library("fdb"), config_(nullptr) {}

LibFdb5& LibFdb5::instance() {
    static LibFdb5 libfdb;
    return libfdb;
}

const Config& LibFdb5::defaultConfig() { 
    if(!config_) {
        Config cfg;
        config_.reset( new Config( std::move(cfg.expandConfig()) ) );
    }
    return *config_;
}

const eckit::Configuration& LibFdb5::userConfig() {
    if(!userConfig_) {
        userConfig_.reset( new eckit::LocalConfiguration() );
    }
    return *userConfig_;
}

void LibFdb5::setUserConfig(const eckit::Configuration& userConfig) {
    userConfig_.reset(new eckit::LocalConfiguration(std::move(userConfig)));
}

bool LibFdb5::dontDeregisterFactories() const {
    return eckit::LibEcKit::instance().dontDeregisterFactories();
}

std::string LibFdb5::version() const {
    return fdb5_version_str();
}

std::string LibFdb5::gitsha1(unsigned int count) const {
    std::string sha1(fdb5_git_sha1());
    if (sha1.empty()) {
        return "not available";
    }

    return sha1.substr(0, std::min(count, 40u));
}

SerialisationVersion LibFdb5::serialisationVersion() const {
    return SerialisationVersion{};
}

RemoteProtocolVersion LibFdb5::remoteProtocolVersion() const {
    return RemoteProtocolVersion{};
}

//----------------------------------------------------------------------------------------------------------------------

static unsigned getUserEnvSerialisationVersion() {

    static unsigned fdbSerialisationVersion =
        eckit::Resource<unsigned>("fdbSerialisationVersion;$FDB5_SERIALISATION_VERSION", 0);
    
    if (fdbSerialisationVersion) {
        eckit::Log::debug() << "fdbSerialisationVersion overidde to version: " << fdbSerialisationVersion << std::endl;
    }
    return fdbSerialisationVersion; // default is 0 (not defined by user/service)
}

SerialisationVersion::SerialisationVersion() {
    static unsigned user = getUserEnvSerialisationVersion();
    // std::cout << "SerialisationVersion user = " << user << std::endl;
    // std::cout << "SerialisationVersion supported = " << supportedStr() << std::endl;
    if (user) {
        bool valid = check(user, false);
        if(not valid) {
            std::ostringstream msg;
            msg << "Unsupported FDB5 serialisation version " << user
            << " - supported: " << supportedStr()
            << std::endl;
            throw eckit::BadValue(msg.str(), Here());
        }
        used_ = user;
    }
    else
        used_ = defaulted();
}

unsigned int SerialisationVersion::latest() const {
    return 3;
}

unsigned int SerialisationVersion::defaulted() const {
    return 2;
}

unsigned int SerialisationVersion::used() const {
    return used_;
}

std::vector<unsigned int> SerialisationVersion::supported() const {
    std::vector<unsigned int> versions = {3, 2, 1};
    return versions;
}

std::string SerialisationVersion::supportedStr() const {
    std::ostringstream oss;
    char sep = '[';
    for (auto v : supported()) {
        oss << sep << v;
        sep = ',';
    }
    oss << ']';
    return oss.str();
}

bool SerialisationVersion::check(unsigned int version, bool throwOnFail) {
    std::vector<unsigned int> versionsSupported = supported();
    for (auto v : versionsSupported) {
        if (version == v)
            return true;
    }
    if (throwOnFail) {
        std::ostringstream msg;
        msg << "Record version mismatch, software supports versions " << supportedStr() << " got " << version;
        throw eckit::SeriousBug(msg.str());
    }
    return false;
}


//----------------------------------------------------------------------------------------------------------------------

static unsigned getUserEnvRemoteProtocol() {

    static unsigned fdbRemoteProtocolVersion =
        eckit::Resource<unsigned>("fdbRemoteProtocolVersion;$FDB5_REMOTE_PROTOCOL_VERSION", 0);
    if (fdbRemoteProtocolVersion) {
        eckit::Log::debug() << "fdbRemoteProtocolVersion overidde to version: " << fdbRemoteProtocolVersion
                            << std::endl;
    }
    return 0;  // no version override
}

static bool getUserEnvSkipSanityCheck() {
    return ::getenv("FDB5_SKIP_REMOTE_PROTOCOL_SANITY_CHECK");
}

RemoteProtocolVersion::RemoteProtocolVersion() {
    static unsigned  user = getUserEnvRemoteProtocol();
    static bool skipcheck = getUserEnvSkipSanityCheck();

    if(not user) {
        used_ = defaulted();
        return;
    }

    if (not skipcheck) {
        bool valid = check(user, false);
        if (not valid) {
            std::ostringstream msg;
            msg << "Unsupported FDB5 remote protocol version " << user << " - supported: " << supportedStr()
                << std::endl;
            throw eckit::BadValue(msg.str(), Here());
        }
    }
    used_ = user;
}

unsigned int RemoteProtocolVersion::latest() const {
    return 3;
}

unsigned int RemoteProtocolVersion::defaulted() const {
    return 3;
}

unsigned int RemoteProtocolVersion::used() const {
    return used_;
}

std::vector<unsigned int> RemoteProtocolVersion::supported() const {
    std::vector<unsigned int> versions = {3};
    return versions;
}

std::string RemoteProtocolVersion::supportedStr() const {
    std::ostringstream oss;
    char sep = '[';
    for (auto v : supported()) {
        oss << sep << v;
        sep = ',';
    }
    oss << ']';
    return oss.str();
}

bool RemoteProtocolVersion::check(unsigned int version, bool throwOnFail) {
    std::vector<unsigned int> versionsSupported = supported();
    for (auto v : versionsSupported) {
        if (version == v)
            return true;
    }
    if (throwOnFail) {
        std::ostringstream msg;
        msg << "Remote protocol version mismaatch, software supports versions " << supportedStr() << " got " << version;
        throw eckit::SeriousBug(msg.str());
    }
    return false;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
