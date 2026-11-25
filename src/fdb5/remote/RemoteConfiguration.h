// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstdint>
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
///
/// "Tracing" lists the supported versions of trace-context propagation (Message::Trace frames). It is only offered
/// when tracing is enabled (by a server: in its configuration; by a client: in the process, see offerTracing()), and
/// only included in the agreed configuration if both sides support a common version. Peers predating it neither send
/// nor return the key, hence never receive Message::Trace.

class RemoteConfiguration {

public:

    RemoteConfiguration() = default;
    RemoteConfiguration(const eckit::Configuration& config);
    RemoteConfiguration(eckit::Stream& s);

    static RemoteConfiguration common(RemoteConfiguration& clientConf, RemoteConfiguration& serverConf);

    bool singleConnection() const;
    uint64_t enabledFeatures() const { return enabledFeatures_; }

    /// Withdraws features (a mask of Message, see toMask()), e.g. those the server grants to some networks only
    void disableFeatures(uint64_t mask) { enabledFeatures_ &= ~mask; }

    /// Offer (or not) trace-context propagation, whatever the configuration
    void offerTracing(bool offer);

    /// Highest supported/agreed trace-context propagation version, 0 if none
    int tracingVersion() const { return tracingVersions_.empty() ? 0 : tracingVersions_.back(); }

    /// The trace-context propagation versions this implementation supports
    static constexpr int tracingProtocolVersion = 1;

    friend eckit::Stream& operator<<(eckit::Stream& s, const RemoteConfiguration& r);
    friend std::ostream& operator<<(std::ostream& s, const RemoteConfiguration& r);

private:

    std::vector<int> remoteFieldLocationVersions_;
    std::vector<int> numberOfConnections_;
    std::vector<int> tracingVersions_;

    std::optional<bool> preferSingleConnection_;

    uint64_t enabledFeatures_{0};

    bool singleConnection_{false};
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
