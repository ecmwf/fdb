#include <functional>
#include <unistd.h>

#include "fdb5/remote/client/ClientConnectionRouter.h"

using namespace eckit;
using namespace eckit::net;


namespace {

// static ClientID generateClientID() {

//     static std::mutex m;
//     static ClientID id = 0;

//     std::lock_guard<std::mutex> lock(m);
//     return ++id;
// }
// static DataLinkID generateDataLinkID() {

//     static std::mutex m;
//     static DataLinkID id = 0;

//     std::lock_guard<std::mutex> lock(m);
//     return ++id;
// }
// static HandlerID generateHandlerID() {

//     static std::mutex m;
//     static HandlerID id = 0;

//     std::lock_guard<std::mutex> lock(m);
//     return ++id;
// }
static uint32_t generateRequestID() {

    static std::mutex m;
    static uint32_t id = 0;

    std::lock_guard<std::mutex> lock(m);
    return ++id;
}

}


namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

uint32_t ClientConnectionRouter::controlWriteCheckResponse(std::vector<eckit::net::Endpoint>& endpoints, Client& client, Message msg, const void* payload, uint32_t payloadLength) {
    //std::cout << "ClientConnectionRouter::controlWriteCheckResponse " << endpoints.size() << std::endl;
    ClientConnection* conn;
    uint32_t id = createConnection(endpoints, client, conn);

    conn->controlWriteCheckResponse(msg, id, payload, payloadLength);

    return id;
}
uint32_t ClientConnectionRouter::controlWrite(std::vector<eckit::net::Endpoint>& endpoints, Client& client, Message msg, const void* payload, uint32_t payloadLength) {
    //std::cout << "ClientConnectionRouter::controlWrite " << endpoints.size() << std::endl;
    ClientConnection* conn;
    uint32_t id = createConnection(endpoints, client, conn);

    conn->controlWrite(msg, id, payload, payloadLength);

    return id;
}
// void ClientConnectionRouter::controlWrite(Client& client, uint32_t requestId, const void* payload, size_t payloadLength) {
//     auto it = requests_.find(requestId);
//     ASSERT(it != requests_.end());
//     ASSERT(it->second.second == &client); // check if the client owns the request

//     it->second.first->controlWrite(payload, payloadLength);
// }
    // void controlRead(Client& client, uint32_t requestId, void* data, size_t length);

    // void dataWrite(Client& client, uint32_t requestId, const void* data, size_t length);
    // void dataRead(Client& client, uint32_t requestId, void* data, size_t length);

void ClientConnectionRouter::controlRead(Client& client, uint32_t requestId, void* payload, size_t payloadLength) {
    // std::cout << "ClientConnectionRouter::controlRead " << requestId << std::endl;
    auto it = requests_.find(requestId);
    ASSERT(it != requests_.end());
    ASSERT(it->second.second == &client); // check if the client owns the request

    it->second.first->controlRead(payload, payloadLength);

}

// void ClientConnectionRouter::controlRead(Connection& connection, Client& client, void* data, size_t length) {

// }

// Client& ClientConnectionRouter::store(eckit::URI uri) {
//     StoreFactory::instance().build()
// }


uint32_t ClientConnectionRouter::createConnection(std::vector<eckit::net::Endpoint>& endpoints, Client& client, ClientConnection*& conn) {

    ASSERT(endpoints.size() > 0);

    // pick the first endpoint - To be improved with DataStoreStrategies
    eckit::net::Endpoint& endpoint = endpoints.at(0);
    uint32_t id = generateRequestID();

    auto it = connections_.find(endpoint.host());
    if (it == connections_.end()) {
        conn = (connections_[endpoint.host()] = std::unique_ptr<ClientConnection>(new ClientConnection(endpoint, LocalConfiguration()))).get();
        conn->connect();
        // client.key();
        // connections_[endpoint] = std::make_pair<std::unique_ptr<ClientConnection>, std::map<Key, uint32_t> >(std::move(conn))
    } else {
        conn = it->second.get();
    }

    ASSERT(conn);
    requests_[id] = {conn, &client};

    return id;
}
uint32_t ClientConnectionRouter::dataWrite(std::vector<eckit::net::Endpoint>& endpoints, Client& client, Message msg, const void* payload, uint32_t payloadLength) {
    ClientConnection* conn;
    uint32_t id = createConnection(endpoints, client, conn);

    conn->dataWrite(msg, id, payload, payloadLength);

    return id;
}
void ClientConnectionRouter::dataWrite(Client& client, uint32_t requestId, const void* payload, size_t payloadLength) {
    auto it = requests_.find(requestId);
    ASSERT(it != requests_.end());
    ASSERT(it->second.second == &client); // check if the client owns the request

    it->second.first->dataWrite(payload, payloadLength);
}
// void ClientConnectionRouter::dataRead(Connection& connection, Client& client, void* data, size_t length) {

// }


bool ClientConnectionRouter::handle(Message message, uint32_t requestID) {
    if (requestID != 0) {
        auto it = requests_.find(requestID);
        ASSERT(it != requests_.end());

        return it->second.second->handle(message, requestID);
    } else {
        std::cout << "ClientConnectionRouter::handle [message=" << ((uint) message) << ",requestID=" << requestID << "]" << std::endl;
        return true;
    }
}

bool ClientConnectionRouter::handle(Message message, uint32_t requestID, eckit::net::Endpoint endpoint, eckit::Buffer&& payload) {
    auto it = requests_.find(requestID);
    ASSERT(it != requests_.end());

    return it->second.second->handle(message, requestID, endpoint, std::move(payload));
}
void ClientConnectionRouter::handleException(std::exception_ptr e) {
    // auto it = requests_.find(requestID);
    // ASSERT(it != requests_.end());

    // it->second->handle(message, requestID);
}


ClientConnectionRouter& ClientConnectionRouter::instance()
{
    static ClientConnectionRouter proxy;
    return proxy;
}

// void ClientConnectionRouter::add(const std::string& name, const FDBBuilderBase* b)
// {
//     eckit::AutoLock<eckit::Mutex> lock(mutex_);

//     ASSERT(registry_.find(name) == registry_.end());

//     registry_[name] = b;
// }

// std::unique_ptr<FDBBase> FDBFactory::build(const Config& config) {

//     // Allow expanding of the config to make use of fdb_home supplied in a previous
//     // configuration file, or to pick up the default configuration from ~fdb/etc/fdb/...

//     Config actualConfig = config.expandConfig();

//     /// We use "local" as a default type if not otherwise configured.

//     std::string key = actualConfig.getString("type", "local");

//     eckit::Log::debug<LibFdb5>() << "Selecting FDB implementation: " << key << std::endl;

//     eckit::AutoLock<eckit::Mutex> lock(mutex_);

//     auto it = registry_.find(key);

//     if (it == registry_.end()) {
//         std::stringstream ss;
//         ss << "FDB factory \"" << key << "\" not found";
//         throw eckit::SeriousBug(ss.str(), Here());
//     }

//     std::unique_ptr<FDBBase> ret = it->second->make(actualConfig);
//     eckit::Log::debug<LibFdb5>() << "Constructed FDB implementation: " << *ret << std::endl;
//     return ret;
// }


}  // namespace fdb5::remote
