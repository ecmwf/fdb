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

#include <map>

#include "eckit/config/Configuration.h"
#include "eckit/memory/NonCopyable.h"
#include "eckit/net/Endpoint.h"
#include "eckit/thread/Mutex.h"

#include "fdb5/database/Store.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/remote/client/Client.h"
#include "fdb5/remote/client/ClientConnection.h"
#include "fdb5/remote/RemoteStore.h"

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

// class Connection {
// public:
//     Connection(ClientConnection* clientConnection, uint32_t remoteID) : clientConnection_(clientConnection), remoteID_(remoteID) {}
//     Connection(Connection& other) : clientConnection_(other.clientConnection_), remoteID_(other.remoteID_) {}
    
//     ClientConnection* clientConnection_;
//     uint32_t remoteID_;
// };

// typedef eckit::net::Endpoint Connection;
// typedef uint32_t ClientID;
// typedef uint32_t DataLinkID;
// typedef uint32_t HandlerID;
// typedef uint32_t RequestID;

class ClientConnectionRouter : eckit::NonCopyable {
public:

    static ClientConnectionRouter& instance();
//    Connection connectCatalogue(Key dbKey, const eckit::Configuration& config);
//    Connection connectStore(Key dbKey, const std::vector<eckit::net::Endpoint>& endpoints);
    RemoteStore& store(const eckit::URI& uri);

// protected:

//     friend class Client;

    uint32_t controlWriteCheckResponse(std::vector<eckit::net::Endpoint>& endpoints, Client& client, Message msg, const void* payload=nullptr, uint32_t payloadLength=0);
    uint32_t controlWrite(std::vector<eckit::net::Endpoint>& connections, Client& client, Message msg, const void* payload=nullptr, uint32_t payloadLength=0);
//    void controlWrite(Client& client, uint32_t requestId, const void* payload, size_t payloadLength);

    // uint32_t controlWriteCheckResponse(Connection& connection, Client& client, Message msg, const void* payload=nullptr, uint32_t payloadLength=0);
    // uint32_t controlWrite(Connection& connection, Client& client, Message msg, const void* payload=nullptr, uint32_t payloadLength=0);
    // void controlWrite(Connection& connection, Client& client, const void* data, size_t length);
    void controlRead(Client& client, uint32_t requestId, void* payload, size_t payloadLength);

    uint32_t dataWrite(std::vector<eckit::net::Endpoint>& endpoints, Client& client, Message msg, const void* payload=nullptr, uint32_t payloadLength=0);
    void dataWrite(Client& client, uint32_t requestId, const void* data, size_t length);
    // void dataRead(Client& client, uint32_t requestId, void* data, size_t length);


    // handlers for incoming messages - to be defined in the client class
    bool handle(Message message, uint32_t requestID);
    bool handle(Message message, uint32_t requestID, eckit::net::Endpoint endpoint, eckit::Buffer&& payload);
    void handleException(std::exception_ptr e);

private:

    ClientConnectionRouter() {} ///< private constructor only used by singleton

    uint32_t createConnection(std::vector<eckit::net::Endpoint>& endpoints, Client& client, ClientConnection*& conn);

    eckit::Mutex mutex_;

    // endpoint --> datalink
    // remoteId --> datalink, remoteHandler
    // a Client is either a CatalogueProxy or a StoreProxy (local),
    // willing to communicate with a remote CatalogueHandler or StoreHandler at a given endpoint
    // std::map<ClientID, std::pair<DataLinkID, HandlerID> > clientLinks_;

    // std::map<eckit::net::Endpoint, std::unique_ptr<ClientConnection> > connections_;

    // std::map<RequestID, Client*> requests_;





    // // key --> [endpoint1, endpoint2, ...]
    // std::map<Key, std::vector<std::pair<eckit::net::Endpoint, uint32_t> > storeEndpoints_;

    // requestID --> <<ClientConnection*, remoteID>, Client>
    std::map<uint32_t, std::pair<ClientConnection*, Client*> > requests_;

    // endpoint -> <client,  key -> remoteId>
//    std::map<eckit::net::Endpoint, std::pair<std::unique_ptr<ClientConnection>, std::map<Key, uint32_t> > connections_;
    std::map<std::string, std::unique_ptr<ClientConnection> > connections_;

    std::map<std::string, std::unique_ptr<RemoteStore> > readStores_;
    
    // // not owning
    // ClientConnection* catalogueConnection_;
};

}