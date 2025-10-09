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

#include "fdb5/remote/Connection.h"
#include "fdb5/remote/Messages.h"

#include "eckit/config/LocalConfiguration.h"
#include "eckit/container/Queue.h"
#include "eckit/io/Buffer.h"
#include "eckit/net/Endpoint.h"
#include "eckit/net/TCPClient.h"
#include "eckit/net/TCPSocket.h"
#include "eckit/runtime/SessionID.h"

#include <future>
#include <thread>

namespace fdb5::remote {

class Client;
class ClientConnectionRouter;
class DataWriteRequest;

//----------------------------------------------------------------------------------------------------------------------

class ClientConnection : protected Connection {

public:  // methods

    ~ClientConnection() override;
    ClientConnection(const eckit::net::Endpoint& controlEndpoint, const std::string& defaultEndpoint);

    std::future<eckit::Buffer> controlWrite(const Client& client, Message msg, uint32_t requestID,
                                            bool /*dataListener*/, PayloadList payload = {}) const;

    void dataWrite(Client& client, Message msg, uint32_t requestID, PayloadList payloads = {});

    void add(Client& client);
    bool remove(uint32_t clientID);

    bool connect(bool singleAttempt = false);
    void disconnect();

    uint32_t generateRequestID();
    const eckit::net::Endpoint& controlEndpoint() const;
    const std::string& defaultEndpoint() const { return defaultEndpoint_; }

    using Connection::valid;

private:  // methods

    friend class ClientConnectionRouter;

    void dataWrite(DataWriteRequest& request) const;

    // construct dictionary for protocol negotiation - to be defined in the client class
    eckit::LocalConfiguration availableFunctionality() const;

    void writeControlStartupMessage();
    void writeDataStartupMessage(const eckit::SessionID& serverSession);

    eckit::SessionID verifyServerStartupResponse();

    // void handleError(const MessageHeader& hdr, eckit::Buffer buffer);

    void listeningControlThreadLoop();
    void listeningDataThreadLoop();
    void dataWriteThreadLoop();
    void closeConnection();

    const eckit::net::TCPSocket& controlSocket() const override { return controlClient_; }

    const eckit::net::TCPSocket& dataSocket() const override { return dataClient_; }

private:  // members

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

    mutable std::mutex promisesMutex_;

    mutable std::map<uint32_t, std::promise<eckit::Buffer>> promises_;

    std::mutex dataWriteMutex_;
    std::unique_ptr<eckit::Queue<DataWriteRequest>> dataWriteQueue_;
    std::thread dataWriteThread_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
