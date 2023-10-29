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
#include "eckit/utils/Translator.h"

namespace eckit {

class Buffer;

// // xxx can we give this code a better home?
// template<> struct Translator<net::Endpoint, std::string> {
//     std::string operator()(const net::Endpoint& e) {
//         std::stringstream ss;
//         ss << e;
//         return ss.str();
//     }
// };

}

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

class ClientConnection : eckit::NonCopyable {

public: // types

public: // methods

    ClientConnection(const eckit::net::Endpoint& controlEndpoint, const eckit::Configuration& config);
    virtual ~ClientConnection();

    void controlWriteCheckResponse(remote::Message msg, uint32_t requestID, const void* payload=nullptr, uint32_t payloadLength=0);
    eckit::Buffer controlWriteReadResponse(remote::Message msg, uint32_t requestID, const void* payload=nullptr, uint32_t payloadLength=0);
    void controlWrite(remote::Message msg, uint32_t requestID, const void* payload=nullptr, uint32_t payloadLength=0);
    void dataWrite(remote::Message msg, uint32_t requestID, const void* payload=nullptr, uint32_t payloadLength=0);

    void controlRead(void* data, size_t length);
    void controlWrite(const void* data, size_t length);

    void dataWrite(const void* data, size_t length);
    void dataRead(void* data, size_t length);

    void connect();
    void disconnect();

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

    void writeControlStartupMessage();
    void writeDataStartupMessage(const eckit::SessionID& serverSession);

    eckit::SessionID verifyServerStartupResponse();

    void handleError(const remote::MessageHeader& hdr);

    void listeningThreadLoop();

private: // members

    eckit::SessionID sessionID_; 

    eckit::net::Endpoint controlEndpoint_;
    eckit::net::Endpoint dataEndpoint_;

    eckit::net::TCPClient controlClient_;
    eckit::net::TCPClient dataClient_;

    std::thread listeningThread_;

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