// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#pragma once

#include <atomic>
#include <exception>
#include <cstdint>
#include <future>
#include <mutex>
#include <string>
#include <utility>
#include <vector>
#include <set>
#include <string_view>

#include "eckit/container/Queue.h"
#include "eckit/io/Buffer.h"
#include "eckit/io/DataHandle.h"
#include "eckit/net/TCPServer.h"
#include "eckit/net/TCPSocket.h"
#include "eckit/runtime/SessionID.h"

#include "fdb5/config/Config.h"
#include "fdb5/remote/Connection.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/remote/RemoteConfiguration.h"

namespace fdb5::remote {

enum class Handled {
    No = 0,
    Yes,
    YesAddArchiveListener,
    YesRemoveArchiveListener,
    YesAddReadListener,
    YesRemoveReadListener,
    Replied,
};

//----------------------------------------------------------------------------------------------------------------------

class Handler {

public:

    virtual Handled handleControl(Message message, uint32_t clientID, uint32_t requestID) = 0;
    virtual Handled handleControl(Message message, uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) = 0;
    virtual Handled handleData(Message message, uint32_t clientID, uint32_t requestID) = 0;
    virtual Handled handleData(Message message, uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) = 0;
};

//----------------------------------------------------------------------------------------------------------------------
// Base class for CatalogueHandler and StoreHandler

struct readLocationElem {
    uint32_t clientID;
    uint32_t requestID;
    std::unique_ptr<eckit::DataHandle> readLocation;

    readLocationElem() : clientID(0), requestID(0), readLocation(nullptr) {}

    readLocationElem(uint32_t clientID, uint32_t requestID, std::unique_ptr<eckit::DataHandle> readLocation) :
        clientID(clientID), requestID(requestID), readLocation(std::move(readLocation)) {}
};

struct ArchiveElem {

    uint32_t clientID_;
    uint32_t requestID_;
    eckit::Buffer payload_;
    bool multiblob_;

    ArchiveElem() : clientID_(0), requestID_(0), payload_(0), multiblob_(false) {}

    ArchiveElem(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload, bool multiblob) :
        clientID_(clientID), requestID_(requestID), payload_(std::move(payload)), multiblob_(multiblob) {}
};

class ServerConnection : public Connection, public Handler {
public:  // methods

    ServerConnection(eckit::net::TCPSocket& socket, const Config& config);
    ~ServerConnection() override;

    /// Handshake with the client. If the client is rejected (e.g. unsupported protocol version), the reason is sent to
    /// it as an Error in place of the Startup reply.
    /// @returns false if the client was rejected: the connection must not be served
    bool initialiseConnections();

    void handle();

    std::string host() const { return controlSocket_.localHost(); }
    int port() const { return controlSocket_.localPort(); }
    const RemoteConfiguration& agreedConf() const { return agreedConf_; }

    Handled handleData(Message message, uint32_t clientID, uint32_t requestID) override;
    Handled handleData(Message message, uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) override;

protected:

    // socket methods
    int selectDataPort();
    RemoteConfiguration availableFunctionality() const;

    /// The features that the "networks" of the configuration can grant to some networks only, by name
    static const std::vector<std::pair<std::string, Message>>& networkRestrictableFeatures();

    // Worker functionality
    void tidyWorkers();
    void waitForWorkers();

    /// Stops and joins all the worker threads (archive, retrieve, API calls). Idempotent.
    /// Workers use the state of the derived handlers, so this must be called while they are still alive: from
    /// handle() on any exit path, and from the destructor of every derived handler (the destructor of
    /// ServerConnection runs after the derived state is gone: it only checks that no worker is left, and terminates
    /// the process otherwise).
    /// @param discardPending drop archive requests that are still queued instead of processing them
    void stopWorkers(bool discardPending) noexcept;

    /// True if a worker thread has not completed (or has not been joined)
    bool workersRunning() noexcept;

    /// While a control message is handled: true if no more archive data can arrive, i.e. on a single connection (its
    /// only reader is handling the control message), or once the data connection has ended
    bool archiveDataEnded() const { return single_ || dataEnded_; }

    /// Number of archive messages queued, and not yet processed by the archive worker
    size_t archivePending() const { return archivePending_; }

    /// Rethrows the failure of the archive worker, if any. Unlike the state of archiveFuture_, this is set before
    /// any client is notified of the failure.
    void rethrowArchiveFailure() const;

    /// Records the first failure of the archive worker
    void recordArchiveFailure(const std::exception_ptr& failure) noexcept;

    /// Terminates the connection after a failure: sends the error (bounded, see tryErrorBounded()) addressed to the
    /// given request, then aborts the connection. Only the first caller does so, the others return immediately: the
    /// thread that detected a failure owns the reporting of it, so that the failures it causes in the other threads
    /// (woken up by the interrupted queue or by the aborted sockets) can neither preempt nor replace its error.
    /// @returns false if the failure was already owned by another thread
    bool failConnection(std::string_view what, uint32_t clientID, uint32_t requestID) noexcept;

    /// True once a thread has taken ownership of a failure (see failConnection()), even if it has not aborted the
    /// connection yet
    bool connectionFailed() const { return failureOwned_ || aborted(); }

    // archival thread
    size_t archiveThreadLoop();

    virtual void archiveBlob(uint32_t clientID, uint32_t requestID, const void* data, size_t length) = 0;

    // archival helper methods
    void archiver();
    void queue(Message message, uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload);

private:

    void listeningThreadLoopData();

    // Initialises the connections and serves control messages until the client disconnects
    void listenControl(std::thread& listeningThreadData);

    const eckit::net::TCPSocket& controlSocket() const override { return controlSocket_; }

    const eckit::net::TCPSocket& dataSocket() const override {
        ASSERT(dataSocket_);
        return *dataSocket_;
    }

protected:

    virtual bool remove(bool control, uint32_t clientID) = 0;

    Config config_;
    Config innerConfig_;

    std::string dataListenHostname_;

    eckit::Queue<readLocationElem> readLocationQueue_;

    eckit::SessionID sessionID_;
    RemoteConfiguration agreedConf_;
    std::mutex readLocationMutex_;
    std::thread readLocationWorker_;

    std::map<uint32_t, std::future<void>> workerThreads_;
    eckit::Queue<ArchiveElem> archiveQueue_;
    std::future<size_t> archiveFuture_;

    eckit::net::TCPSocket controlSocket_;
    std::string clientNetwork_;     // the network of the client, if the configuration of the server has "networks"
    uint64_t disabledFeatures_{0};  // the features restricted to networks the client is not part of

    /// Guards the state of the clients in the derived handlers (and removedClients_). It is held only to look up,
    /// add or remove a client: users of the state of a client pin it (shared_ptr), so that a Stop can remove it
    /// while a worker is still using it.
    std::mutex handlerMutex_;

    /// Clients that have been removed (Stop). The client may have gone without a flush: what is still queued for it
    /// is discarded. Caller holds handlerMutex_.
    std::set<uint32_t> removedClients_;
    bool removedClient(uint32_t clientID) const { return removedClients_.count(clientID) != 0; }
    size_t numControlConnection_{0};
    size_t numDataConnection_{0};

private:

    std::mutex dataPortMutex_;

    mutable std::mutex archiveFailureMutex_;
    std::exception_ptr archiveFailure_;
    std::atomic<bool> discardArchive_{false};
    std::atomic<bool> failureOwned_{false};
    std::atomic<size_t> archivePending_{0};
    std::atomic<bool> dataEnded_{false};

    // data connection
    std::unique_ptr<eckit::net::EphemeralTCPServer> dataSocket_;

    size_t numDataListener_{0};
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
