#include "fdb5/remote/client/ClientConnectionRouter.h"

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

ClientConnection* ClientConnectionRouter::connection(Client& client) {

    // pick the first endpoint - To be improved with DataStoreStrategies
    const eckit::net::Endpoint& endpoint = client.controlEndpoint();

    std::lock_guard<std::mutex> lock(connectionMutex_);

    auto it = connections_.find(endpoint.hostport());
    if (it != connections_.end()) {
        it->second.clients_.insert(&client);
        return it->second.connection_.get();
    } else {
        auto it = (connections_.emplace(endpoint.hostport(), Connection(std::unique_ptr<ClientConnection>(new ClientConnection{endpoint}), client))).first;
        it->second.connection_->connect();
        return it->second.connection_.get();
    }
}

void ClientConnectionRouter::deregister(Client& client) {
    const eckit::net::Endpoint& endpoint = client.controlEndpoint();

    std::lock_guard<std::mutex> lock(connectionMutex_);

    auto it = connections_.find(endpoint.hostport());
    ASSERT(it != connections_.end());

    auto clientIt = it->second.clients_.find(&client);
    ASSERT(clientIt != it->second.clients_.end());

    it->second.clients_.erase(clientIt);
    if (it->second.clients_.empty()) {
        connections_.erase(it);
    }
}

ClientConnectionRouter& ClientConnectionRouter::instance()
{
    static ClientConnectionRouter router;
    return router;
}

}  // namespace fdb5::remote
