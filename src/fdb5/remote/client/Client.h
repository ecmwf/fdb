// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "fdb5/remote/Connection.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/remote/client/ClientConnection.h"

#include "eckit/net/Endpoint.h"
#include "eckit/utils/Literals.h"

#include <atomic>
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <utility>  // std::pair
#include <vector>

using namespace eckit::literals;

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

class RemoteFDBException : public eckit::RemoteException {
public:

    RemoteFDBException(const std::string& msg, const eckit::net::Endpoint& endpoint) :
        eckit::RemoteException(msg, endpoint) {}
};

class RemoteFDBUnauthorised : public eckit::RemoteException {
public:

    RemoteFDBUnauthorised(const std::string& msg, const eckit::net::Endpoint& endpoint) :
        eckit::RemoteException(msg, endpoint) {}
};

/// The server explicitly rejected the connection during the handshake (e.g. protocol or configuration mismatch, not
/// authorised): retrying will not help
class RemoteFDBRejected : public RemoteFDBException {
public:

    RemoteFDBRejected(const std::string& msg, const eckit::net::Endpoint& endpoint) :
        RemoteFDBException(msg, endpoint) {}
};

/// The server did not answer in time (e.g. overloaded): it may later
class RemoteFDBTimeout : public RemoteFDBException {
public:

    RemoteFDBTimeout(const std::string& msg, const eckit::net::Endpoint& endpoint) :
        RemoteFDBException(msg, endpoint) {}

    bool retryOnClient() const override { return true; }
};

//----------------------------------------------------------------------------------------------------------------------

class Client {
public:  // types

    using PayloadList = Connection::PayloadList;
    using EndpointList = std::vector<std::pair<eckit::net::Endpoint, std::string>>;

    static constexpr size_t defaultBufferSizeArchive = 8_KiB;
    static constexpr size_t defaultBufferSizeFlush = 1_KiB;
    static constexpr size_t defaultBufferSizeKey = 4_KiB;

public:  // methods

    Client(const eckit::Configuration& config);
    Client(const eckit::Configuration& config, const eckit::net::Endpoint& endpoint,
           const std::string& defaultEndpoint);
    Client(const eckit::Configuration& config, const EndpointList& endpoints);

    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;
    Client(Client&&) = delete;
    Client& operator=(Client&&) = delete;

    virtual ~Client();

    uint32_t clientId() const { return id_; }

    uint32_t id() const { return id_; }

    const eckit::net::Endpoint& controlEndpoint() const { return connection_->controlEndpoint(); }

    const std::string& defaultEndpoint() const { return connection_->defaultEndpoint(); }

    uint32_t generateRequestID() const { return connection_->generateRequestID(); }

    // blocking requests
    void controlWriteCheckResponse(Message msg, uint32_t requestID, bool dataListener, const void* payload = nullptr,
                                   uint32_t payloadLength = 0) const;

    eckit::Buffer controlWriteReadResponse(Message msg, uint32_t requestID, const void* payload = nullptr,
                                           uint32_t payloadLength = 0) const;

    void dataWrite(Message msg, uint32_t requestID, PayloadList payloads = {});

    virtual const eckit::Configuration& clientConfig() const = 0;

    // handlers for incoming messages - to be defined in the client class
    virtual bool handle(Message message, uint32_t requestID) = 0;
    virtual bool handle(Message message, uint32_t requestID, eckit::Buffer&& payload) = 0;
    /// Called when the connection is lost: wake up whoever is waiting on it.
    /// @param reason why the connection was lost (e.g. the error reported by the server). Never null.
    virtual void closeConnection(const std::exception_ptr& /*reason*/) {}

    // Create a new connection if the current one is invalid
    void refreshConnection();

    /// False once the connection is lost (or torn down): requests would fail
    bool connectionUsable() const { return connection_->usable(); }

protected:

    // Deregister this client from its connection. Idempotent.
    // Derived classes accessed by handle() should call this
    // in their destructor, before that state is destroyed.
    void deregister();

    /// Wraps the reason of a connection loss with some context, keeping the original message (e.g. the error
    /// reported by the server), so that callers woken by closeConnection() still see why the connection was lost.
    std::exception_ptr closureError(const std::string& context, const std::exception_ptr& reason) const;

    std::shared_ptr<ClientConnection> connection_;

private:

    void setClientID();

    // rethrows any exception as a RemoteFDBException.
    eckit::Buffer waitControlResponse(std::future<eckit::Buffer>& response, Message msg, uint32_t requestID) const;

private:

    uint32_t id_;
    std::atomic<bool> deregistered_{false};
    mutable std::mutex blockingRequestMutex_;
};

}  // namespace fdb5::remote
