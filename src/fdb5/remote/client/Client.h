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

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

class RemoteFDBException : public eckit::RemoteException {
public:
    RemoteFDBException(const std::string& msg, const eckit::net::Endpoint& endpoint):
        eckit::RemoteException(msg, endpoint.hostport()) {}
};

//----------------------------------------------------------------------------------------------------------------------

class Client : eckit::NonCopyable {
public:
    Client(const eckit::net::Endpoint& endpoint);

    const eckit::net::Endpoint& controlEndpoint() { return endpoint_; }

    static uint32_t generateRequestID();

    uint32_t controlWriteCheckResponse(Message msg, const void* payload=nullptr, uint32_t payloadLength=0, uint32_t requestID = generateRequestID());
    eckit::Buffer controlWriteReadResponse(Message msg, const void* payload=nullptr, uint32_t payloadLength=0);
    uint32_t controlWrite(Message msg, const void* payload=nullptr, uint32_t payloadLength=0);

    uint32_t dataWrite(Message msg, const void* payload=nullptr, uint32_t payloadLength=0);
    void dataWrite(uint32_t requestId, const void* data, size_t length);

    // handlers for incoming messages - to be defined in the client class
    virtual bool handle(Message message, uint32_t requestID) = 0;
    virtual bool handle(Message message, uint32_t requestID, eckit::net::Endpoint endpoint, eckit::Buffer&& payload) = 0;
    virtual void handleException(std::exception_ptr e) = 0;

protected:

    eckit::net::Endpoint endpoint_;

};

}