// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "fdb5/remote/Connection.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/remote/RemoteConfiguration.h"

#include "eckit/container/Queue.h"
#include "eckit/io/Buffer.h"
#include "eckit/net/Endpoint.h"
#include "eckit/net/TCPClient.h"
#include "eckit/net/TCPSocket.h"
#include "eckit/runtime/SessionID.h"

#include <cstdint>
#include <exception>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace eckit {

class Configuration;

}

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

    // Blocks until the data-writer thread has processed all data writes queued before this call.
    void flushDataWrites();

    // Registers the client; false if the connection is already torn down, then get another from the router.
    [[nodiscard]] bool add(Client& client);
    bool remove(uint32_t clientID);

    bool connect(const eckit::Configuration& config, bool singleAttempt = false);
    void disconnect();

    uint32_t generateRequestID();
    const eckit::net::Endpoint& controlEndpoint() const;
    const std::string& defaultEndpoint() const { return defaultEndpoint_; }

    using Connection::valid;

private:  // methods

    friend class ClientConnectionRouter;

    void dataWrite(DataWriteRequest& request) const;

    // construct dictionary for protocol negotiation - to be defined in the client class
    RemoteConfiguration availableFunctionality(const eckit::Configuration& config) const;

    void writeControlStartupMessage(const eckit::Configuration& config);
    void writeDataStartupMessage(const eckit::SessionID& serverSession);

    eckit::SessionID verifyServerStartupResponse();

    // void handleError(const MessageHeader& hdr, eckit::Buffer buffer);

    void listeningControlThreadLoop();
    void listeningDataThreadLoop();

    // Tears down *only this* connection
    void handleConnectionError(const std::exception_ptr& eptr);

    // do not hang forever once the connection is known to be dead.
    void failPendingRequests(const std::exception_ptr& eptr);

    // fail any waiting flush data barriers
    void failDataBarriers(const std::exception_ptr& eptr);

    void dataWriteThreadLoop();
    void closeConnection();

    const eckit::net::TCPSocket& controlSocket() const override { return controlClient_; }

    const eckit::net::TCPSocket& dataSocket() const override { return dataClient_; }

    void checkEnabled(Message msg) const;

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

    // requestID
    std::mutex idMutex_;
    uint32_t id_;

    bool connected_;

    mutable std::mutex promisesMutex_;

    mutable std::map<uint32_t, std::promise<eckit::Buffer>> promises_;

    std::mutex dataWriteMutex_;
    std::unique_ptr<eckit::Queue<DataWriteRequest>> dataWriteQueue_;
    std::thread dataWriteThread_;

    uint64_t agreedFeatures_{0};

    std::vector<std::shared_ptr<std::promise<void>>> dataBarriers_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
