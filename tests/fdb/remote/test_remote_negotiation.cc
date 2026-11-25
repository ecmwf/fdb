// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

// Negotiation of optional remote protocol features at connection startup, in particular trace-context propagation,
// which must stay off whenever either peer predates it or has it disabled.

#include "eckit/config/LocalConfiguration.h"
#include "eckit/io/Buffer.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/testing/Test.h"
#include "eckit/value/Value.h"

#include "fdb5/remote/Messages.h"
#include "fdb5/remote/RemoteConfiguration.h"
#include "fdb5/tracing/Tracer.h"

namespace fdb5::test {

using fdb5::remote::RemoteConfiguration;

namespace {

/// Simulate the wire: serialise and deserialise, as client and server do at startup
RemoteConfiguration overTheWire(const RemoteConfiguration& conf) {
    eckit::Buffer buffer(4096);
    eckit::MemoryStream out(buffer);
    out << conf;
    eckit::MemoryStream in(buffer);
    return RemoteConfiguration{in};
}

/// The startup dictionary sent by an implementation that predates trace-context propagation
RemoteConfiguration legacyPeer() {
    eckit::Value val = eckit::Value::makeOrderedMap();
    val["RemoteFieldLocation"] = eckit::toValue(std::vector<int>{1});
    val["NumberOfConnections"] = eckit::toValue(std::vector<int>{1, 2});
    val["EnabledFeatures"] = eckit::toValue(remote::maskOfDefaultFeatures);

    eckit::Buffer buffer(4096);
    eckit::MemoryStream out(buffer);
    out << val;
    eckit::MemoryStream in(buffer);
    return RemoteConfiguration{in};
}

/// The configuration offered by a peer whose process has tracing enabled (or not)
RemoteConfiguration withTracing(bool enabled) {
    auto configureEnabled = [](bool e) {
        eckit::LocalConfiguration tracing;
        tracing.set("enabled", e);
        tracing::Tracer::instance().configure(tracing);
    };
    configureEnabled(enabled);
    RemoteConfiguration conf{eckit::LocalConfiguration{}};
    configureEnabled(true);
    return conf;
}

bool sendsTracingKey(const RemoteConfiguration& conf) {
    eckit::Buffer buffer(4096);
    eckit::MemoryStream out(buffer);
    out << conf;
    eckit::MemoryStream in(buffer);
    eckit::Value v(in);
    return v.contains("Tracing");
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

CASE("Tracing is offered by default and agreed between two current peers") {
    RemoteConfiguration client = overTheWire(RemoteConfiguration{eckit::LocalConfiguration{}});
    RemoteConfiguration server{eckit::LocalConfiguration{}};

    EXPECT_EQUAL(client.tracingVersion(), RemoteConfiguration::tracingProtocolVersion);

    RemoteConfiguration agreed = overTheWire(RemoteConfiguration::common(client, server));
    EXPECT_EQUAL(agreed.tracingVersion(), RemoteConfiguration::tracingProtocolVersion);
}

CASE("Current client, legacy server: tracing is off") {
    // A legacy server builds its agreed configuration from the keys it knows: 'Tracing' is never returned.
    // Equivalently, the legacy server's own capabilities lack the key:
    RemoteConfiguration client = overTheWire(withTracing(true));
    RemoteConfiguration server = legacyPeer();

    RemoteConfiguration agreed = overTheWire(RemoteConfiguration::common(client, server));
    EXPECT_EQUAL(agreed.tracingVersion(), 0);
    EXPECT(!sendsTracingKey(agreed));
}

CASE("Legacy client, current server: tracing is off and the reply carries no new key") {
    RemoteConfiguration client = legacyPeer();
    RemoteConfiguration server = withTracing(true);

    RemoteConfiguration agreed = RemoteConfiguration::common(client, server);
    EXPECT_EQUAL(agreed.tracingVersion(), 0);
    EXPECT(!sendsTracingKey(agreed));
}

CASE("Tracing disabled on either side: not offered, not agreed") {
    EXPECT(!sendsTracingKey(withTracing(false)));
    EXPECT(sendsTracingKey(withTracing(true)));

    {
        RemoteConfiguration client = overTheWire(withTracing(false));
        RemoteConfiguration server = withTracing(true);
        EXPECT_EQUAL(RemoteConfiguration::common(client, server).tracingVersion(), 0);
    }
    {
        RemoteConfiguration client = overTheWire(withTracing(true));
        RemoteConfiguration server = withTracing(false);
        EXPECT_EQUAL(RemoteConfiguration::common(client, server).tracingVersion(), 0);
    }
}

CASE("Existing negotiated features are unaffected") {
    RemoteConfiguration client = overTheWire(withTracing(true));
    RemoteConfiguration server = legacyPeer();
    RemoteConfiguration agreed = overTheWire(RemoteConfiguration::common(client, server));
    EXPECT_EQUAL(agreed.enabledFeatures(), remote::maskOfDefaultFeatures);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::test

int main(int argc, char** argv) {
    return ::eckit::testing::run_tests(argc, argv);
}
