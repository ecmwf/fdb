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

class ClientConnectionRouter : eckit::NonCopyable {
public:

    static ClientConnectionRouter& instance();
    RemoteStore& store(const eckit::URI& uri);

    uint32_t controlWriteCheckResponse(Client& client, Message msg, const void* payload=nullptr, uint32_t payloadLength=0);
    uint32_t controlWrite(Client& client, Message msg, const void* payload=nullptr, uint32_t payloadLength=0);
    void controlRead(Client& client, uint32_t requestId, void* payload, size_t payloadLength);

    uint32_t dataWrite(Client& client, Message msg, const void* payload=nullptr, uint32_t payloadLength=0);
    void dataWrite(Client& client, uint32_t requestId, const void* data, size_t length);

    // handlers for incoming messages - to be defined in the client class
    bool handle(Message message, uint32_t requestID);
    bool handle(Message message, uint32_t requestID, eckit::net::Endpoint endpoint, eckit::Buffer&& payload);
    void handleException(std::exception_ptr e);

private:

    ClientConnectionRouter() {} ///< private constructor only used by singleton

    uint32_t createConnection(Client& client, ClientConnection*& conn);

    eckit::Mutex mutex_;

    std::map<uint32_t, std::pair<ClientConnection*, Client*> > requests_;

    // endpoint -> connection
    std::map<std::string, std::unique_ptr<ClientConnection> > connections_;

    // endpoint -> Store (one read store for each connection. Do not need to have one for each key)
    std::map<std::string, std::unique_ptr<RemoteStore> > readStores_;

};

}