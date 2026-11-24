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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <set>
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

    /// When the server aborts a connection it first sends the reason on the control socket, which only the control
    /// listener reads, while a failure on the data socket is seen by other threads (data listener, data writer),
    /// possibly earlier. Waits until the control listener has stopped, i.e. until everything the server sent before
    /// the abort has been processed, so that the reason can be reported instead of the generic failure.
    /// Bounded, as the control socket may be healthy.
    /// @returns false if the control listener is still running after the timeout
    bool awaitControlListener(std::chrono::milliseconds timeout = std::chrono::seconds(2));

    /// Can be given to a new client: valid, and not retired (its last client is gone, it is being torn down)
    bool usable() const { return valid() && !retired_; }

    /// Why the connection was lost (e.g. an error with clientID == 0 sent by the server), once the loss has been
    /// handled. Null if the connection has not failed, or was closed without a reason.
    std::exception_ptr connectionError() const;

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

    // Reads the answer of the server to the startup message, as a whole within fdbStartupTimeout
    eckit::Buffer readStartupResponse(MessageHeader& hdr);
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

    // Wakes up all the clients, passing on why the connection was lost
    void closeConnection(const std::exception_ptr& reason);

    // The reason recorded by handleConnectionError(), or a generic one if the connection was simply closed
    std::exception_ptr closureReason();

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

    // A registered client. While pinned (a message is being dispatched to it, or it is being told the connection is
    // lost), it cannot be removed, i.e. destroyed. A client is never called with clientsMutex_ held: a call may block
    // (e.g. on the full queue of a read), and the mutex must remain available to report a failure to the others.
    // A client being removed is retiring: no new message is dispatched to it, but it is still told if the connection
    // is lost, as one of its calls may be blocked until its requests are interrupted.
    struct ClientEntry {
        explicit ClientEntry(Client& c) : client(c) {}
        Client& client;
        size_t pins{0};         // guarded by clientsMutex_
        bool retiring{false};   // guarded by clientsMutex_
    };

    // Calls f(Client&) without holding clientsMutex_, with the client pinned
    // @param includeRetiring also call a client that is being removed (to notify it, not to dispatch a message)
    // @returns false if no such client is registered
    template <typename F>
    bool withClient(uint32_t clientID, F&& f, bool includeRetiring = false);

    void unpin(ClientEntry& entry);

    // A message for a client that has been removed (or is being removed) is discarded: the server may still be
    // sending for it. A message for a client that was never registered is a protocol error.
    bool wasRemoved(uint32_t clientID);

    std::mutex clientsMutex_;
    std::condition_variable clientsCondition_;  // notified when a client is unpinned
    std::map<uint32_t, std::shared_ptr<ClientEntry>> clients_;
    std::set<uint32_t> removedClients_;  // guarded by clientsMutex_
    std::atomic<bool> retired_{false};   // set under clientsMutex_

    std::thread listeningControlThread_;
    std::thread listeningDataThread_;

    // The last listener to stop releases the sockets: the connection is over, but the object may live on for long
    // (e.g. held by a retired client), and its descriptors must not
    std::atomic<int> runningListeners_{0};
    void listenerStopped() noexcept;

    mutable std::mutex controlListenerMutex_;
    std::condition_variable controlListenerCondition_;
    bool controlListenerDone_{false};
    std::exception_ptr connectionError_;  // guarded by controlListenerMutex_

    // requestID
    std::mutex idMutex_;
    uint32_t id_;

    bool connected_;

    mutable std::mutex promisesMutex_;

    mutable std::map<uint32_t, std::promise<eckit::Buffer>> promises_;
    // Once the pending requests have been failed, the control listener has stopped: no new request may be registered,
    // nobody would answer it. Guarded by promisesMutex_, so that a request is either failed or rejected.
    mutable bool promisesFailed_{false};

    std::mutex dataWriteMutex_;
    std::unique_ptr<eckit::Queue<DataWriteRequest>> dataWriteQueue_;
    std::thread dataWriteThread_;

    uint64_t agreedFeatures_{0};
    uint64_t offeredFeatures_{0};  // the features allowed by the configuration of this client

    std::vector<std::shared_ptr<std::promise<void>>> dataBarriers_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
