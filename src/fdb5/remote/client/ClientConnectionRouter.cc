#include <functional>
#include <unistd.h>

#include "fdb5/remote/client/ClientConnectionRouter.h"

using namespace eckit;
using namespace eckit::net;


namespace {

static uint32_t generateRequestID() {

    static std::mutex m;
    static uint32_t id = 0;

    std::lock_guard<std::mutex> lock(m);
    return ++id;
}

}


namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

RemoteStore& ClientConnectionRouter::store(const eckit::URI& uri) {
    const std::string& endpoint = uri.hostport();
    auto it = readStores_.find(endpoint);
    if (it != readStores_.end()) {
        return *(it->second);
    }

    return *(readStores_[endpoint] = std::unique_ptr<RemoteStore>(new RemoteStore(uri, Config())));
}

uint32_t ClientConnectionRouter::createConnection(Client& client, ClientConnection*& conn) {

    // pick the first endpoint - To be improved with DataStoreStrategies
    const eckit::net::Endpoint& endpoint = client.controlEndpoint();
    uint32_t id = generateRequestID();

    auto it = connections_.find(endpoint.hostport());
    if (it != connections_.end()) {
        conn = it->second.get();
    } else {
        conn = (connections_[endpoint.hostport()] = std::unique_ptr<ClientConnection>(new ClientConnection(endpoint, LocalConfiguration()))).get();
        conn->connect();
    }

    ASSERT(conn);
    requests_[id] = {conn, &client};

    return id;
}

uint32_t ClientConnectionRouter::controlWriteCheckResponse(Client& client, Message msg, const void* payload, uint32_t payloadLength) {
    //std::cout << "ClientConnectionRouter::controlWriteCheckResponse " << endpoints.size() << std::endl;
    ClientConnection* conn;
    uint32_t id = createConnection(client, conn);

    conn->controlWriteCheckResponse(msg, id, payload, payloadLength);

    return id;
}

void ClientConnectionRouter::controlReadResponse(Client& client, remote::Message msg, void* payload, uint32_t& payloadLength) {
    ClientConnection* conn;
    uint32_t id = createConnection(client, conn);

    conn->controlReadResponse(msg, id, payload, payloadLength);
}

uint32_t ClientConnectionRouter::controlWrite(Client& client, Message msg, const void* payload, uint32_t payloadLength) {
    //std::cout << "ClientConnectionRouter::controlWrite " << endpoints.size() << std::endl;
    ClientConnection* conn;
    uint32_t id = createConnection(client, conn);

    conn->controlWrite(msg, id, payload, payloadLength);

    return id;
}
void ClientConnectionRouter::controlRead(Client& client, uint32_t requestId, void* payload, size_t payloadLength) {

    auto it = requests_.find(requestId);
    ASSERT(it != requests_.end());
    ASSERT(it->second.second == &client); // check if the client owns the request

    it->second.first->controlRead(payload, payloadLength);

}

uint32_t ClientConnectionRouter::dataWrite(Client& client, Message msg, const void* payload, uint32_t payloadLength) {
    ClientConnection* conn;
    uint32_t id = createConnection(client, conn);

    conn->dataWrite(msg, id, payload, payloadLength);

    return id;
}
void ClientConnectionRouter::dataWrite(Client& client, uint32_t requestId, const void* payload, size_t payloadLength) {
    auto it = requests_.find(requestId);
    ASSERT(it != requests_.end());
    ASSERT(it->second.second == &client); // check if the client owns the request

    it->second.first->dataWrite(payload, payloadLength);
}

bool ClientConnectionRouter::handle(Message message, uint32_t requestID) {

    if (requestID != 0) {
        auto it = requests_.find(requestID);
        ASSERT(it != requests_.end());

        return it->second.second->handle(message, requestID);
    } else {
        Log::error() << "ClientConnectionRouter::handle [message=" << ((uint) message) << ",requestID=" << requestID << "]" << std::endl;
        return true;
    }
}
bool ClientConnectionRouter::handle(Message message, uint32_t requestID, eckit::net::Endpoint endpoint, eckit::Buffer&& payload) {

    if (requestID != 0) {
        auto it = requests_.find(requestID);
        ASSERT(it != requests_.end());
    
        return it->second.second->handle(message, requestID, endpoint, std::move(payload));
    } else {
        Log::error() << "ClientConnectionRouter::handle [message=" << ((uint) message) << ",requestID=" << requestID << "]" << std::endl;
        return true;
    }
}
void ClientConnectionRouter::handleException(std::exception_ptr e) {
    // auto it = requests_.find(requestID);
    // ASSERT(it != requests_.end());

    // it->second->handle(message, requestID);
}

ClientConnectionRouter& ClientConnectionRouter::instance()
{
    static ClientConnectionRouter router;
    return router;
}

}  // namespace fdb5::remote