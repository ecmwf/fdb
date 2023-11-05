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
#include <set>

// #include "eckit/config/Configuration.h"
// #include "eckit/memory/NonCopyable.h"
// #include "eckit/net/Endpoint.h"
// #include "eckit/thread/Mutex.h"

// #include "fdb5/remote/Messages.h"
#include "fdb5/remote/client/Client.h"
#include "fdb5/remote/client/ClientConnection.h"

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

class Connection {
public:

    Connection(std::unique_ptr<ClientConnection> connection, Client& clients)
        : connection_(std::move(connection)), clients_(std::set<Client*>{&clients}) {}

    std::unique_ptr<ClientConnection> connection_;
    std::set<Client*> clients_;
};

//----------------------------------------------------------------------------------------------------------------------

class ClientConnectionRouter : eckit::NonCopyable {
public:

    static ClientConnectionRouter& instance();
    // static uint32_t generateRequestID();

    // uint32_t controlWriteCheckResponse(Client& client, Message msg, uint32_t requestID = generateRequestID(), const void* payload=nullptr, uint32_t payloadLength=0);
    // eckit::Buffer controlWriteReadResponse(Client& client, remote::Message msg, const void* payload=nullptr, uint32_t payloadLength=0);
    // uint32_t controlWrite(Client& client, Message msg, const void* payload=nullptr, uint32_t payloadLength=0);
    // void controlRead(Client& client, uint32_t requestId, void* payload, size_t payloadLength);

    // uint32_t dataWrite(Client& client, Message msg, const void* payload=nullptr, uint32_t payloadLength=0);
    // void dataWrite(Client& client, uint32_t requestId, const void* data, size_t length);

    // // handlers for incoming messages - to be defined in the client class
    // bool handle(Message message, uint32_t requestID);
    // bool handle(Message message, uint32_t requestID, eckit::net::Endpoint endpoint, eckit::Buffer&& payload);
    // void handleException(std::exception_ptr e);

    ClientConnection* connection(Client& client);

    void deregister(Client& client);

private:

    ClientConnectionRouter() {} ///< private constructor only used by singleton


    std::mutex connectionMutex_;

    // std::map<Client*, std::vector<uint32_t>> currentRequests_;
    // std::map<ClientConnection*, size_t>

    // endpoint -> connection
    std::map<std::string, Connection> connections_;
};

}