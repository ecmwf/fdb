#include "fdb5/remote/client/ClientConnectionRouter.h"

#include "fdb5/remote/client/ClientConnection.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/log/Log.h"
#include "eckit/net/Endpoint.h"

#include <cstddef>
#include <memory>
#include <mutex>
#include <ostream>
#include <random>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace {

class ConnectionError : public eckit::Exception {
public:

    ConnectionError();
    ConnectionError(const eckit::net::Endpoint&);

    bool retryOnClient() const override { return true; }
};

ConnectionError::ConnectionError() {
    std::ostringstream s;
    s << "Unable to create a connection with the FDB server";
    reason(s.str());
    eckit::Log::status() << what() << std::endl;
}

ConnectionError::ConnectionError(const eckit::net::Endpoint& endpoint) {
    std::ostringstream s;
    s << "Unable to create a connection with the FDB endpoint " << endpoint;
    reason(s.str());
    eckit::Log::status() << what() << std::endl;
}
}  // namespace
namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

std::shared_ptr<ClientConnection> ClientConnectionRouter::connection(const eckit::Configuration& config,
                                                                     const eckit::net::Endpoint& endpoint,
                                                                     const std::string& defaultEndpoint) {
    std::lock_guard lock(connectionMutex_);
    reap();
    if (const auto iter = connections_.find(endpoint); iter != connections_.end()) {
        if (auto conn = iter->second.lock(); conn && conn->valid()) {
            return conn;
        }
    }
    auto clientConnection = std::make_shared<ClientConnection>(endpoint, defaultEndpoint);
    if (clientConnection->connect(config)) {
        connections_.insert_or_assign(endpoint, clientConnection);
        return clientConnection;
    }
    throw ConnectionError(endpoint);
}

std::shared_ptr<ClientConnection> ClientConnectionRouter::connection(
    const eckit::Configuration& config, const std::vector<std::pair<eckit::net::Endpoint, std::string>>& endpoints) {

    std::vector<std::pair<eckit::net::Endpoint, std::string>> fullEndpoints{endpoints};

    std::lock_guard lock(connectionMutex_);
    reap();
    while (fullEndpoints.size() > 0) {
        // select a random endpoint
        thread_local std::mt19937 rndGen{std::random_device{}()};
        size_t idx = std::uniform_int_distribution<size_t>(0, fullEndpoints.size() - 1)(rndGen);
        eckit::net::Endpoint endpoint = fullEndpoints.at(idx).first;

        // look for the selected endpoint (a dead connection must not be handed out; replace it)
        if (const auto it = connections_.find(endpoint); it != connections_.end()) {
            if (auto conn = it->second.lock(); conn && conn->valid()) {
                return conn;
            }
        }

        // not yet there, trying to connect
        auto clientConnection = std::make_shared<ClientConnection>(endpoint, fullEndpoints.at(idx).second);
        if (clientConnection->connect(config, true)) {
            connections_.insert_or_assign(endpoint, clientConnection);
            return clientConnection;
        }

        // unable to connect to "endpoint", remove it and try again
        if (idx != fullEndpoints.size() - 1) {  // swap with the last element
            fullEndpoints[idx] = std::move(fullEndpoints.back());
        }
        fullEndpoints.pop_back();
    }

    // no more available endpoints, we have to give up
    throw ConnectionError();
}

std::shared_ptr<ClientConnection> ClientConnectionRouter::refresh(const eckit::Configuration& config,
                                                                  const std::shared_ptr<ClientConnection>& connection) {
    std::lock_guard lock(connectionMutex_);
    reap();
    if (const auto iter = connections_.find(connection->controlEndpoint()); iter != connections_.end()) {
        // Another client may already have refreshed this endpoint: reuse the live connection.
        if (auto conn = iter->second.lock(); conn && conn->valid()) {
            return conn;
        }
    }
    auto newConnection =
        std::make_shared<ClientConnection>(connection->controlEndpoint(), connection->defaultEndpoint());
    if (newConnection->connect(config)) {
        // Replacing keeps a single pooled connection per endpoint.
        connections_.insert_or_assign(newConnection->controlEndpoint(), newConnection);
        return newConnection;
    }
    throw ConnectionError(newConnection->controlEndpoint());
}

void ClientConnectionRouter::reap() {
    for (auto iter = connections_.begin(); iter != connections_.end();) {
        auto conn = iter->second.lock();
        if (!conn || !conn->valid()) {
            iter = connections_.erase(iter);
        }
        else {
            ++iter;
        }
    }
}

void ClientConnectionRouter::deregister(ClientConnection& connection) {
    std::lock_guard lock(connectionMutex_);
    const auto iter = connections_.find(connection.controlEndpoint());
    if (iter != connections_.end()) {
        auto conn = iter->second.lock();
        if (!conn || conn.get() == &connection) {
            connections_.erase(iter);
        }
    }
}

ClientConnectionRouter& ClientConnectionRouter::instance() {
    // Leaked deliberately: avoids racing this destructor against other threads tearing
    // down connections during static/process deinitialisation.
    static ClientConnectionRouter& router = *new ClientConnectionRouter();
    return router;
}

ClientConnectionRouter::~ClientConnectionRouter() {
    // there is nothing to tear down here; as a courtesy we notify the server for those.
    std::lock_guard lock(connectionMutex_);
    for (auto& [endp, weak] : connections_) {
        if (auto conn = weak.lock()) {
            eckit::Log::warning() << "closing connection " << endp << std::endl;
            conn->teardown();
        }
    }
    connections_.clear();
}

}  // namespace fdb5::remote
