#include "fdb5/remote/client/ClientConnectionRouter.h"

namespace{
    
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
}
namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

ClientConnection* ClientConnectionRouter::connection(Client& client, const FdbEndpoint& endpoint) {

    std::lock_guard<std::mutex> lock(connectionMutex_);
    auto it = connections_.find(endpoint);
    if (it != connections_.end()) {
        it->second.clients_.insert(&client);
        return it->second.connection_.get();
    } else {
        ClientConnection* clientConnection = new ClientConnection(endpoint);
        if (clientConnection->connect()) {
            auto it = (connections_.emplace(endpoint, Connection(std::unique_ptr<ClientConnection>(clientConnection), client))).first;
            return it->second.connection_.get();
        } else {
            throw ConnectionError(endpoint);
        }
    }
}

ClientConnection* ClientConnectionRouter::connection(Client& client, const std::vector<FdbEndpoint>& endpoints) {

    std::vector<FdbEndpoint> fullEndpoints{endpoints};

    std::lock_guard<std::mutex> lock(connectionMutex_);
    while (fullEndpoints.size()>0) {

        // select a random endpoint
        size_t idx = std::rand() % fullEndpoints.size();
        FdbEndpoint endpoint = fullEndpoints.at(idx);

        // look for the selected endpoint
        auto it = connections_.find(endpoint);
        if (it != connections_.end()) {
            it->second.clients_.insert(&client);
            return it->second.connection_.get();
        }
        else { // not yet there, trying to connect
            ClientConnection* clientConnection = new ClientConnection(fullEndpoints.at(idx));
            if (clientConnection->connect(true)) {
                auto it = (connections_.emplace(endpoint, Connection(std::unique_ptr<ClientConnection>(clientConnection), client))).first;
                return it->second.connection_.get();
            }
        }

        // unable to connect to "endpoint", remove it and try again
        if (idx != fullEndpoints.size()-1) { // swap with the last element
            fullEndpoints[idx] = std::move(fullEndpoints.back());
        }
        fullEndpoints.pop_back();
    }

    // no more available endpoints, we have to give up
    throw ConnectionError();
}


void ClientConnectionRouter::deregister(Client& client) {
    std::lock_guard<std::mutex> lock(connectionMutex_);

    auto it = connections_.find(client.controlEndpoint());
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
