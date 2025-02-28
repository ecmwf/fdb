#include "fdb5/remote/client/ClientConnectionRouter.h"

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

std::shared_ptr<ClientConnection> ClientConnectionRouter::connection(const eckit::net::Endpoint& endpoint,
                                                                     const std::string& defaultEndpoint) {
    std::lock_guard<std::mutex> lock(connectionMutex_);

    const auto it = connections_.find(endpoint);
    if (it != connections_.end()) {
        return (it->second);
    }
    else {
        auto clientConnection = std::make_shared<ClientConnection>(endpoint, defaultEndpoint);
        if (clientConnection->connect()) {
            const auto it = (connections_.emplace(endpoint, clientConnection)).first;
            return (it->second);
        }
        else {
            throw ConnectionError(endpoint);
        }
    }
}

std::shared_ptr<ClientConnection> ClientConnectionRouter::connection(
    const std::vector<std::pair<eckit::net::Endpoint, std::string>>& endpoints) {

    std::vector<std::pair<eckit::net::Endpoint, std::string>> fullEndpoints{endpoints};

    std::lock_guard<std::mutex> lock(connectionMutex_);
    while (fullEndpoints.size() > 0) {

        // select a random endpoint
        size_t idx                    = std::rand() % fullEndpoints.size();
        eckit::net::Endpoint endpoint = fullEndpoints.at(idx).first;

        // look for the selected endpoint
        const auto it = connections_.find(endpoint);
        if (it != connections_.end()) {
            return (it->second);
        }
        else {  // not yet there, trying to connect
            auto clientConnection = std::make_shared<ClientConnection>(endpoint, fullEndpoints.at(idx).second);
            if (clientConnection->connect(true)) {
                const auto it = (connections_.emplace(endpoint, std::move(clientConnection))).first;
                return (it->second);
            }
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

void ClientConnectionRouter::deregister(ClientConnection& connection) {

    const auto it = connections_.find(connection.controlEndpoint());
    if (it != connections_.end()) {
        connections_.erase(it);
    }
}

ClientConnectionRouter& ClientConnectionRouter::instance() {
    static ClientConnectionRouter router;
    return router;
}

void ClientConnectionRouter::teardown(std::exception_ptr e) {

    try {
        if (e) {
            std::rethrow_exception(e);
        }
    }
    catch (const std::exception& e) {
        eckit::Log::error() << "error: " << e.what();
    }

    std::lock_guard<std::mutex> lock(connectionMutex_);

    for (const auto& [endp, conn] : connections_) {
        if (conn) {
            eckit::Log::warning() << "closing connection " << endp << std::endl;
            conn->teardown();
        }
    }
}

}  // namespace fdb5::remote
