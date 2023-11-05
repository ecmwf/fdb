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

#include <thread>

#include "eckit/config/LocalConfiguration.h"
#include "eckit/container/Queue.h"
#include "eckit/io/Buffer.h"
#include "eckit/io/Length.h"
#include "eckit/net/Endpoint.h"
#include "eckit/net/TCPClient.h"
#include "eckit/net/TCPStream.h"
#include "eckit/runtime/SessionID.h"

#include "fdb5/remote/Messages.h"

namespace eckit {

class Buffer;

}

namespace fdb5::remote {

class Client;

//----------------------------------------------------------------------------------------------------------------------

class ClientConnection : eckit::NonCopyable {

public: // types

public: // methods

    ClientConnection(const eckit::net::Endpoint& controlEndpoint);
    virtual ~ClientConnection();

    // void controlWriteCheckResponse(remote::Message msg, uint32_t requestID, const void* payload=nullptr, uint32_t payloadLength=0);
    // eckit::Buffer controlWriteReadResponse(remote::Message msg, uint32_t requestID, const void* payload=nullptr, uint32_t payloadLength=0);
    void controlWrite(Client& client, remote::Message msg, uint32_t requestID, std::vector<std::pair<const void*, uint32_t>> data={});
    void dataWrite(remote::Message msg, uint32_t requestID, std::vector<std::pair<const void*, uint32_t>> data={});

    void connect();
    void disconnect();

    uint32_t generateRequestID();

protected: // methods

    const eckit::net::Endpoint& controlEndpoint() const;
    const eckit::net::Endpoint& dataEndpoint() const;
    
    // // handlers for incoming messages - to be defined in the client class
    // virtual void handle(Message message, uint32_t requestID) = 0;
    // virtual void handle(Message message, uint32_t requestID, eckit::Buffer&& payload) = 0;
    // virtual void handleException(std::exception_ptr e) = 0;

    // construct dictionary for protocol negotiation - to be defined in the client class
    virtual eckit::LocalConfiguration availableFunctionality() const;

private: // methods

    void controlWrite(Message msg, uint32_t requestID = 0, std::vector<std::pair<const void*, uint32_t>> data = {});
    void controlWrite(const void* data, size_t length);
    void controlRead (      void* data, size_t length);
    void dataWrite   (const void* data, size_t length);
    void dataRead    (      void* data, size_t length);
 
    void addRequest(Client& client, uint32_t requestId);

    void writeControlStartupMessage();
    void writeDataStartupMessage(const eckit::SessionID& serverSession);

    eckit::SessionID verifyServerStartupResponse();

    void handleError(const MessageHeader& hdr);

    void listeningThreadLoop();

private: // members

    eckit::SessionID sessionID_; 

    eckit::net::Endpoint controlEndpoint_;
    eckit::net::Endpoint dataEndpoint_;

    eckit::net::TCPClient controlClient_;
    eckit::net::TCPClient dataClient_;

    std::map<uint32_t, Client*> requests_;

    std::thread listeningThread_;
    
    std::mutex controlMutex_;
    std::mutex dataMutex_;

    // requestId
    std::mutex idMutex_;
    uint32_t id_;

    bool connected_;
    
};

//----------------------------------------------------------------------------------------------------------------------

// // // n.b. if we get integer overflow, we reuse the IDs. This is not a
// // //      big deal. The idea that we could be on the 4.2 billionth (successful)
// // //      request, and still have an ongoing request 0 is ... laughable.
// static uint32_t generateRequestID() {

//     static std::mutex m;
//     static uint32_t id = 0;

//     std::lock_guard<std::mutex> lock(m);
//     return ++id;
// }


}  // namespace fdb5::remote