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

#include <future>
#include <thread>

#include "eckit/container/Queue.h"
#include "eckit/io/Buffer.h"
#include "eckit/net/Endpoint.h"
#include "eckit/net/TCPClient.h"
#include "eckit/net/TCPStream.h"
#include "eckit/runtime/SessionID.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/FDBFactory.h"
#include "fdb5/remote/Messages.h"
#include "eckit/utils/Translator.h"


namespace eckit {
// xxx can we give this code a better home?
template<> struct Translator<net::Endpoint, std::string> {
    std::string operator()(const net::Endpoint& e) {
        std::stringstream ss;
        ss << e;
        return ss.str();
    }
};
}

namespace fdb5 {

//class DecoupledFDB;

namespace remote {

//----------------------------------------------------------------------------------------------------------------------

class ClientConnection : eckit::NonCopyable {

public: // types

public:

    ClientConnection(const eckit::net::Endpoint& controlEndpoint, const eckit::Configuration& config);
    ~ClientConnection();

    void setDataEndpoint(const eckit::net::Endpoint& dataEndpoint);
    void connect();
    void disconnect();

    const eckit::net::Endpoint& controlEndpoint() const { 
        return controlEndpoint_;
    } 
    const eckit::net::Endpoint& dataEndpoint() const { 
        return dataEndpoint_;
    } 
    // const eckit::net::Endpoint& dataEndpoint() const { 
    //     return dataEndpoint_;
    // }
protected:

    virtual void listeningThreadLoop() = 0;

    // Construct dictionary for protocol negotiation
    // im not sure if this belongs here, or in the above class
    eckit::LocalConfiguration availableFunctionality() const;

    void writeControlStartupMessage();
    void writeDataStartupMessage(const eckit::SessionID& serverSession);
    eckit::SessionID verifyServerStartupResponse();
    
    void controlWriteCheckResponse(remote::Message msg, uint32_t requestID, const void* payload=nullptr, uint32_t payloadLength=0);
    void controlWrite(remote::Message msg, uint32_t requestID, const void* payload=nullptr, uint32_t payloadLength=0);
    void controlWrite(const void* data, size_t length);
    void controlRead(void* data, size_t length);
    void dataWrite(remote::Message msg, uint32_t requestID, const void* payload=nullptr, uint32_t payloadLength=0);
    void dataWrite(const void* data, size_t length);
    void dataRead(void* data, size_t length);
    void handleError(const remote::MessageHeader& hdr);
    
    void index(const Key& key, const FieldLocation& location);
    // void store(const Key& key, const void* data, size_t length);
    // void archive(const Key& key, const void* data, size_t length);
    // long sendArchiveData(uint32_t id, const std::vector<std::pair<Key, eckit::Buffer>>& elements, size_t count);
    // void sendArchiveData(uint32_t id, const Key& key, const void* data, size_t length);
    // void flush(FDBStats& stats);

private:
    eckit::SessionID sessionID_; 

    eckit::net::Endpoint controlEndpoint_;
    eckit::net::Endpoint dataEndpoint_;
    eckit::net::TCPClient controlClient_;
    eckit::net::TCPClient dataClient_;
    std::thread listeningThread_;

    bool connected_;

//    friend class fdb5::DecoupledFDB;
};

//----------------------------------------------------------------------------------------------------------------------

// // n.b. if we get integer overflow, we reuse the IDs. This is not a
// //      big deal. The idea that we could be on the 2.1 billionth (successful)
// //      request, and still have an ongoing request 0 is ... laughable.
static uint32_t generateRequestID() {

    static std::mutex m;
    static uint32_t id = 0;

    std::lock_guard<std::mutex> lock(m);
    return ++id;
}

class DecoupledFDBException : public eckit::RemoteException {
public:
    DecoupledFDBException(const std::string& msg, const eckit::net::Endpoint& endpoint):
        eckit::RemoteException(msg, eckit::Translator<eckit::net::Endpoint, std::string>()(endpoint)) {}
};

}  // namespace remote
}  // namespace fdb5