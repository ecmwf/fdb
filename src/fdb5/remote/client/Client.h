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

#include "eckit/config/Configuration.h"
#include "eckit/memory/NonCopyable.h"
#include "eckit/net/Endpoint.h"

#include "fdb5/database/Key.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/remote/client/ClientConnection.h"

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

class RemoteFDBException : public eckit::RemoteException {
public:
    RemoteFDBException(const std::string& msg, const eckit::net::Endpoint& endpoint):
        eckit::RemoteException(msg, endpoint) {}
};

//----------------------------------------------------------------------------------------------------------------------

class Client : eckit::NonCopyable {
public:
    Client(const eckit::net::Endpoint& endpoint, const std::string& defaultEndpoint);
    Client(const std::vector<std::pair<eckit::net::Endpoint, std::string>>& endpoints);
    virtual ~Client();

    uint32_t clientId() const { return id_; }
    uint32_t id() const { return id_; }
    const eckit::net::Endpoint& controlEndpoint() const { return connection_.controlEndpoint(); }
    const std::string& defaultEndpoint() const { return connection_.defaultEndpoint(); }

    uint32_t generateRequestID() { return connection_.generateRequestID(); }

    // blocking requests
    void controlWriteCheckResponse(Message msg, uint32_t requestID, bool dataListener, const void* payload=nullptr, uint32_t payloadLength=0);
    eckit::Buffer controlWriteReadResponse (Message msg, uint32_t requestID, const void* payload=nullptr, uint32_t payloadLength=0);

    void dataWrite(remote::Message msg, uint32_t requestID, std::vector<std::pair<const void*, uint32_t>> data={});
    
    // handlers for incoming messages - to be defined in the client class
    virtual bool handle(Message message, uint32_t requestID) = 0;
    virtual bool handle(Message message, uint32_t requestID, eckit::Buffer&& payload) = 0;

protected:
    
    ClientConnection& connection_;

private:
    void setClientID();

private:
    uint32_t id_;

    std::mutex blockingRequestMutex_;
};

}