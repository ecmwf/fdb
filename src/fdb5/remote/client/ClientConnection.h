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
#include <future>

#include "eckit/config/LocalConfiguration.h"
#include "eckit/container/Queue.h"
#include "eckit/io/Buffer.h"
#include "eckit/io/Length.h"
#include "eckit/net/TCPClient.h"
#include "eckit/net/TCPStream.h"
#include "eckit/runtime/SessionID.h"

#include "fdb5/remote/Messages.h"
#include "fdb5/remote/Connection.h"

namespace fdb5::remote {

class Client;
class ClientConnectionRouter;
class DataWriteRequest;

//----------------------------------------------------------------------------------------------------------------------

class ClientConnection : protected Connection {

public: // methods

    virtual ~ClientConnection();

    std::future<eckit::Buffer> controlWrite(Client& client, Message msg, uint32_t requestID, bool startDataListener, std::vector<std::pair<const void*, uint32_t>> data={});
    void dataWrite(Client& client, Message msg, uint32_t requestID, std::vector<std::pair<const void*, uint32_t>> data={});

    void add(Client& client);
    bool remove(uint32_t clientID);

    bool connect(bool singleAttempt = false);
    void disconnect();

    uint32_t generateRequestID();
    const eckit::net::Endpoint& controlEndpoint() const;
    const std::string& defaultEndpoint() const { return defaultEndpoint_; }

private: // methods

    friend class ClientConnectionRouter;

    ClientConnection(const eckit::net::Endpoint& controlEndpoint, const std::string& defaultEndpoint);

    void dataWrite(DataWriteRequest& dataWriteRequest);

    // construct dictionary for protocol negotiation - to be defined in the client class
    eckit::LocalConfiguration availableFunctionality() const;

    void writeControlStartupMessage();
    void writeDataStartupMessage(const eckit::SessionID& serverSession);

    eckit::SessionID verifyServerStartupResponse();

    void handleError(const MessageHeader& hdr, eckit::Buffer buffer);

    void listeningControlThreadLoop();
    void listeningDataThreadLoop();
    void dataWriteThreadLoop();

    eckit::net::TCPSocket& controlSocket() override { return controlClient_; }
    eckit::net::TCPSocket& dataSocket() override { return dataClient_; }

private: // members

    eckit::SessionID sessionID_; 

    eckit::net::Endpoint controlEndpoint_;
    eckit::net::Endpoint dataEndpoint_;

    std::string defaultEndpoint_;

    eckit::net::TCPClient controlClient_;
    eckit::net::TCPClient dataClient_;

    std::mutex clientsMutex_;
    std::map<uint32_t, Client*> clients_;

    std::thread listeningControlThread_;
    std::thread listeningDataThread_;
    
    std::mutex requestMutex_;

    // requestID
    std::mutex idMutex_;
    uint32_t id_;

    bool connected_; 
    bool controlStopping_; 
    bool dataStopping_; 

    std::mutex promisesMutex_;
    std::map<uint32_t, std::promise<eckit::Buffer>> promises_;

    std::mutex dataWriteMutex_;
    std::unique_ptr<eckit::Queue<DataWriteRequest>> dataWriteQueue_;
    std::thread dataWriteThread_;
    // std::future<void> dataWriteFuture_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote