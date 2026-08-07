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

#include "fdb5/remote/client/ClientConnection.h"

#include "eckit/net/Endpoint.h"

#include <memory>
#include <mutex>
#include <unordered_map>

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

    ClientConnectionRouter() {}  ///< private constructor only used by singleton

    /// Drop entries whose connection has been destroyed (expired) or invalidated.
    /// Caller must hold connectionMutex_.
    void reap();

    std::mutex connectionMutex_;

    /// @note ClientConnections are owned by the Client objects.
    /// dead slots are never handed out and purged lazily by reap().
    std::unordered_map<eckit::net::Endpoint, std::weak_ptr<ClientConnection>> connections_;
};

}  // namespace fdb5::remote
