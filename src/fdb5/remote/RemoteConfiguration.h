/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#pragma once

#include <optional>
#include <string>
#include <vector>

#include "eckit/exception/Exceptions.h"

namespace eckit {

class Configuration;
class Stream;
class Value;

}  // namespace eckit

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

class RemoteConnectionNegotiationException : public eckit::Exception {
public:

    RemoteConnectionNegotiationException(const std::string& msg, const eckit::CodeLocation& here) :
        eckit::Exception(std::string("RemoteConnectionNegotiationException: ") + msg, here) {}
};

//----------------------------------------------------------------------------------------------------------------------

/// We use a dictionary to negotiate FDB remote protocol:
/// * the server recives a dictionary describing client capabilities
/// * server computes the intersection of client and server capabilities and then sends the enabled features to
///   the client
/// On-the-wire protocol is thus independent of the FDB version:  we negotiate using the values which happen to be
/// present. This is to make rolling forward easier (i.e. if a client/server doesn't provide a value, then we know
/// that it isn't supported).

class RemoteConfiguration {

public:

    RemoteConfiguration() = default;
    RemoteConfiguration(const eckit::Configuration& config);
    RemoteConfiguration(eckit::Stream& s);

    static RemoteConfiguration common(RemoteConfiguration& clientConf, RemoteConfiguration& serverConf);

    bool singleConnection() const;

    friend eckit::Stream& operator<<(eckit::Stream& s, const RemoteConfiguration& r);

private:

    std::vector<int> remoteFieldLocationVersions_;
    std::vector<int> numberOfConnections_;

    std::optional<bool> preferSingleConnection_;

    bool singleConnection_{false};
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
