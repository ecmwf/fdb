// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "fdb5/remote/client/ClientConnection.h"

#include "eckit/net/Endpoint.h"

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>


namespace eckit {
class Configuration;
}

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------
size_t random(size_t const max);

class ClientConnectionRouter {
public:

    static ClientConnectionRouter& instance();

    ~ClientConnectionRouter();

    ClientConnectionRouter(const ClientConnectionRouter&) = delete;
    ClientConnectionRouter& operator=(const ClientConnectionRouter&) = delete;
    ClientConnectionRouter(ClientConnectionRouter&&) = delete;
    ClientConnectionRouter& operator=(ClientConnectionRouter&&) = delete;

    std::shared_ptr<ClientConnection> connection(const eckit::Configuration& config,
                                                 const eckit::net::Endpoint& endpoint,
                                                 const std::string& defaultEndpoint);
    std::shared_ptr<ClientConnection> connection(
        const eckit::Configuration& config, const std::vector<std::pair<eckit::net::Endpoint, std::string>>& endpoints);

    std::shared_ptr<ClientConnection> refresh(const eckit::Configuration& config,
                                              const std::shared_ptr<ClientConnection>& connection);

    void deregister(ClientConnection& connection);

private:

    ClientConnectionRouter();  ///< private constructor only used by singleton

    // A connection is identified by its endpoint and its default endpoint (used to build the field locations)
    using Key = std::pair<std::string, std::string>;
    static Key key(const eckit::net::Endpoint& endpoint, const std::string& defaultEndpoint);

    /// The cached connection, if it is usable. Caller must hold connectionMutex_.
    std::shared_ptr<ClientConnection> cached(const Key& key);

    /// A usable connection, cached or new (connected without holding connectionMutex_)
    /// @returns null if the server cannot be reached; @throws if it rejects the connection
    std::shared_ptr<ClientConnection> obtain(const eckit::Configuration& config, const eckit::net::Endpoint& endpoint,
                                             const std::string& defaultEndpoint, bool singleAttempt);

    /// Drop entries whose connection has been destroyed (expired), invalidated or retired.
    /// Caller must hold connectionMutex_.
    void reap();

    /// Guards connections_ only: never held while connecting (or doing any other I/O)
    std::mutex connectionMutex_;

    /// @note ClientConnections are owned by the Client objects.
    /// dead slots are never handed out and purged lazily by reap().
    std::map<Key, std::weak_ptr<ClientConnection>> connections_;
};

}  // namespace fdb5::remote
