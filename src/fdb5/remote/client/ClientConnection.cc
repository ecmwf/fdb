
#include "fdb5/remote/client/ClientConnection.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/remote/Connection.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/remote/RemoteConfiguration.h"
#include "fdb5/remote/client/Client.h"
#include "fdb5/remote/client/ClientConnectionRouter.h"

#include "eckit/config/LocalConfiguration.h"
#include "eckit/config/Resource.h"
#include "eckit/container/Queue.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/io/Buffer.h"
#include "eckit/log/Bytes.h"
#include "eckit/log/CodeLocation.h"
#include "eckit/log/Log.h"
#include "eckit/net/Endpoint.h"
#include "eckit/runtime/SessionID.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/utils/Literals.h"

#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <exception>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

using namespace eckit;
using namespace eckit::literals;

namespace fdb5::remote {

namespace {
std::string msgHeader(MessageHeader& hdr, net::Endpoint& endpoint) {
    std::ostringstream ss;
    ss << (hdr.control() ? "CONTROL" : "DATA") << " connection=" << endpoint << " [message=" << hdr.message
       << ",clientID=" << hdr.clientID() << ",requestID=" << hdr.requestID << ",payload=" << hdr.payloadSize << "]";
    return ss.str();
}
}  // namespace

//----------------------------------------------------------------------------------------------------------------------

class DataWriteRequest {

public:

    DataWriteRequest() = default;

    DataWriteRequest(Client* client, Message msg, uint32_t id, Buffer&& data) :
        client_(client), msg_(msg), id_(id), data_(std::move(data)) {}

    /// @param barrier: no payload, signals when all writes are finished
    explicit DataWriteRequest(std::shared_ptr<std::promise<void>> barrier) : barrier_{std::move(barrier)} {}

    Client* client_{nullptr};
    Message msg_{Message::None};
    uint32_t id_{0};
    Buffer data_{Buffer(0)};
    std::shared_ptr<std::promise<void>> barrier_;
};

//----------------------------------------------------------------------------------------------------------------------

ClientConnection::ClientConnection(const net::Endpoint& controlEndpoint, const std::string& defaultEndpoint) :
    controlEndpoint_(controlEndpoint),
    defaultEndpoint_(defaultEndpoint),
    id_(1),
    connected_(false),
    dataWriteQueue_(nullptr) {

    LOG_DEBUG_LIB(LibFdb5) << "ClientConnection::ClientConnection() controlEndpoint: " << controlEndpoint << std::endl;
}

void ClientConnection::add(Client& client) {
    std::lock_guard lock(clientsMutex_);
    clients_[client.id()] = &client;
}

bool ClientConnection::remove(uint32_t clientID) {
    std::lock_guard lock(clientsMutex_);

    if (clientID > 0) {

        auto it = clients_.find(clientID);

        if (it != clients_.end()) {
            if (valid()) {
                try {
                    Connection::write(Message::Stop, true, clientID, 0);
                }
                catch (...) {
                    Log::error() << "ClientConnection::remove() - failed to send STOP message to server for clientID "
                                 << clientID << std::endl;
                }
            }

            clients_.erase(it);
        }
    }

    if (clients_.empty()) {
        teardown();
        ClientConnectionRouter::instance().deregister(*this);
    }

    return clients_.empty();
}

ClientConnection::~ClientConnection() {
    {
        std::lock_guard lock(dataWriteMutex_);
        if (dataWriteQueue_) {
            dataWriteQueue_->close();
        }
    }

    disconnect();
}

uint32_t ClientConnection::generateRequestID() {
    std::lock_guard<std::mutex> lock(idMutex_);
    // we do not want to re-use previous request IDs
    ASSERT(id_ < UINT32_MAX - 2);
    return ++id_;
}

bool ClientConnection::connect(const Configuration& config, bool singleAttempt) {

    if (connected_) {
        Log::warning() << "ClientConnection::connect() called when already connected" << std::endl;
        return connected_;
    }

    int fdbMaxConnectRetries = (singleAttempt ? 1 : Resource<int>("fdbMaxConnectRetries", 3));
    int fdbConnectTimeout = Resource<int>("fdbConnectTimeout", (singleAttempt ? 2 : 5));  // 0 = No timeout

    try {
        // Connect to server, and check that the server is happy on the response
        LOG_DEBUG_LIB(LibFdb5) << "Connecting to host: " << controlEndpoint_ << std::endl;
        controlClient_.connect(controlEndpoint_, fdbMaxConnectRetries, fdbConnectTimeout);

        writeControlStartupMessage(config);
        SessionID serverSession = verifyServerStartupResponse();

        if (!single_) {
            // Connect to the specified data port
            LOG_DEBUG_LIB(LibFdb5) << "Received data endpoint from host: " << dataEndpoint_ << std::endl;
            dataClient_.connect(dataEndpoint_, fdbMaxConnectRetries, fdbConnectTimeout);
            writeDataStartupMessage(serverSession);

            listeningDataThread_ = std::thread([this] { listeningDataThreadLoop(); });
        }

        listeningControlThread_ = std::thread([this] { listeningControlThreadLoop(); });

        connected_ = true;
    }
    catch (TooManyRetries& e) {
        if (controlClient_.isConnected()) {
            controlClient_.close();
        }
    }
    return connected_;
}

void ClientConnection::disconnect() {

    {
        std::lock_guard lock(clientsMutex_);
        ASSERT(clients_.empty());
    }

    if (connected_) {
        if (dataWriteThread_.joinable()) {
            dataWriteThread_.join();
        }
        if (listeningDataThread_.joinable()) {
            listeningDataThread_.join();
        }
        if (listeningControlThread_.joinable()) {
            listeningControlThread_.join();
        }

        // Close both the control and data connections
        dataClient_.close();
        controlClient_.close();
        connected_ = false;
    }
}

const net::Endpoint& ClientConnection::controlEndpoint() const {
    return controlEndpoint_;
}

RemoteConfiguration ClientConnection::availableFunctionality(const Configuration& config) const {
    return RemoteConfiguration{config};
}

//----------------------------------------------------------------------------------------------------------------------

std::future<Buffer> ClientConnection::controlWrite(const Client& client, const Message msg, const uint32_t requestID,
                                                   const bool /*dataListener*/, const PayloadList payloads) const {
    if (!valid()) {
        throw RemoteFDBException("Connection to " + std::string(controlEndpoint_) + " is no longer valid",
                                 controlEndpoint_);
    }

    std::future<Buffer> f;
    {
        std::lock_guard lock(promisesMutex_);
        auto pp = promises_.emplace(requestID, std::promise<Buffer>{}).first;
        f = pp->second.get_future();
    }
    Connection::write(msg, true, client.clientId(), requestID, payloads);

    return f;
}

void ClientConnection::dataWrite(DataWriteRequest& request) const {
    Connection::write(request.msg_, false, request.client_->clientId(), request.id_, request.data_.data(),
                      request.data_.size());
}

void ClientConnection::dataWrite(Client& client, remote::Message msg, uint32_t requestID, PayloadList payloads) {

    static size_t maxQueueLength = Resource<size_t>("fdbDataWriteQueueLength;$FDB_DATA_WRITE_QUEUE_LENGTH", 320);

    {
        // retrieve or add client to the list
        std::lock_guard lock(clientsMutex_);
        ASSERT(clients_.find(client.clientId()) != clients_.end());
    }

    {
        std::lock_guard lock(dataWriteMutex_);
        if (!dataWriteThread_.joinable()) {
            // Reset the queue after previous done/errors
            ASSERT(!dataWriteQueue_);

            dataWriteQueue_ = std::make_unique<Queue<DataWriteRequest>>(maxQueueLength);
            dataWriteThread_ = std::thread([this] { dataWriteThreadLoop(); });
        }
    }

    uint32_t payloadLength = 0;
    for (const auto& payload : payloads) {
        ASSERT(payload.data);
        payloadLength += payload.length;
    }

    Buffer buffer{payloadLength};
    uint32_t offset = 0;
    for (const auto& payload : payloads) {
        buffer.copy(payload.data, payload.length, offset);
        offset += payload.length;
    }

    dataWriteQueue_->emplace(&client, msg, requestID, std::move(buffer));
}

void ClientConnection::dataWriteThreadLoop() {

    Timer timer;
    DataWriteRequest element;

    try {

        ASSERT(dataWriteQueue_);
        while (dataWriteQueue_->pop(element) != -1) {
            if (element.barrier_) {
                element.barrier_->set_value();  // unblock the waiting thread
                continue;
            }
            dataWrite(element);
        }

        dataWriteQueue_.reset();
    }
    catch (...) {
        dataWriteQueue_->interrupt(std::current_exception());
        throw;
    }

    // We are inside an async, so don't need to worry about exceptions escaping.
    // They will be released when flush() is called.
}

void ClientConnection::flushDataWrites() {
    std::shared_ptr<std::promise<void>> barrier;
    std::future<void> written;
    {
        std::lock_guard lock(dataWriteMutex_);
        if (!dataWriteQueue_) {
            return;
        }
        barrier = std::make_shared<std::promise<void>>();
        written = barrier->get_future();
        dataWriteQueue_->emplace(barrier);
    }
    // block the thread!
    written.get();
}

void ClientConnection::writeControlStartupMessage(const Configuration& config) {

    Buffer payload(4096);
    MemoryStream s(payload);
    s << sessionID_;
    s << net::Endpoint(controlEndpoint_.hostname(), controlEndpoint_.port());
    s << LibFdb5::instance().remoteProtocolVersion().used();
    s << availableFunctionality(config);

    LOG_DEBUG_LIB(LibFdb5) << "writeControlStartupMessage - Sending session " << sessionID_ << " to control "
                           << controlEndpoint_ << std::endl;
    Connection::write(Message::Startup, true, 0, 0, payload, s.position());
}

void ClientConnection::writeDataStartupMessage(const SessionID& serverSession) {

    Buffer payload(1_KiB);
    MemoryStream s(payload);

    s << sessionID_;
    s << serverSession;

    LOG_DEBUG_LIB(LibFdb5) << "writeDataStartupMessage - Sending session " << sessionID_ << " to data " << dataEndpoint_
                           << std::endl;
    Connection::write(Message::Startup, false, 0, 0, payload, s.position());
}

SessionID ClientConnection::verifyServerStartupResponse() {

    MessageHeader hdr;
    Buffer payload = Connection::readControl(hdr);

    ASSERT(hdr.requestID == 0);

    MemoryStream s(payload);
    SessionID clientSession(s);
    SessionID serverSession(s);
    net::Endpoint dataEndpoint(s);
    LocalConfiguration serverFunctionality(s);

    dataEndpoint_ = dataEndpoint;

    LOG_DEBUG_LIB(LibFdb5) << "verifyServerStartupResponse - Received from server " << clientSession << " "
                           << serverSession << " " << dataEndpoint << std::endl;
    if (dataEndpoint_.hostname() != controlEndpoint_.hostname()) {
        Log::warning() << "Data and control interface hostnames do not match. " << dataEndpoint_.hostname()
                       << " /= " << controlEndpoint_.hostname() << std::endl;
    }

    if (clientSession != sessionID_) {
        std::ostringstream ss;
        ss << "Session ID does not match session received from server: " << sessionID_ << " != " << clientSession;
        throw BadValue(ss.str(), Here());
    }
    if (serverFunctionality.has("NumberOfConnections") && serverFunctionality.getInt("NumberOfConnections") == 1) {
        single_ = true;
    }

    if (single_ && !(dataEndpoint_ == controlEndpoint_)) {
        Log::warning() << "Returned control interface does not match. " << dataEndpoint_ << " /= " << controlEndpoint_
                       << std::endl;
    }

    return serverSession;
}

void ClientConnection::failPendingRequests(const std::exception_ptr& eptr) {
    std::lock_guard lock(promisesMutex_);
    for (auto& [requestID, promise] : promises_) {
        promise.set_exception(eptr);
    }
    promises_.clear();
}

void ClientConnection::handleConnectionError(const std::exception_ptr& eptr) {
    std::string reason;
    try {
        if (eptr) {
            std::rethrow_exception(eptr);
        }
    }
    catch (const std::exception& e) {
        reason = e.what();
        Log::error() << "error: " << reason << std::endl;
    }
    catch (...) {
        reason = "unknown exception";
        Log::error() << "error: unknown exception on connection " << controlEndpoint_ << std::endl;
    }

    // nothing else will ever fulfil a promise once the listening thread has stopped
    std::ostringstream ss;
    ss << "Connection to " << controlEndpoint_ << " lost: " << reason;
    failPendingRequests(std::make_exception_ptr(RemoteFDBException(ss.str(), controlEndpoint_)));

    teardown();
}

void ClientConnection::listeningControlThreadLoop() {

    try {

        MessageHeader hdr;

        while (true) {

            Buffer payload = Connection::readControl(hdr);

            LOG_DEBUG_LIB(LibFdb5) << "ClientConnection::listeningControlThreadLoop - "
                                   << msgHeader(hdr, controlEndpoint_) << std::endl;

            if (hdr.message == Message::Exit) {
                LOG_DEBUG_LIB(LibFdb5) << "ClientConnection::listeningControlThreadLoop() -- Control thread stopping"
                                       << std::endl;
                return;
            }
            if (hdr.clientID()) {

                ASSERT(hdr.control() || single_);

                bool handled = false;
                if (hdr.control()) {
                    // Hold promisesMutex_ only for the promise lookup/erase (NOT across handle(); risk a deadlock.
                    std::lock_guard lock(promisesMutex_);

                    auto pp = promises_.find(hdr.requestID);
                    if (pp != promises_.end()) {
                        if (hdr.message == Message::Received) {
                            if (hdr.payloadSize == 0) {
                                pp->second.set_value(Buffer(0));
                            }
                            else {
                                pp->second.set_value(std::move(payload));
                            }
                        }
                        else {  // Message::Error or Message::Unauthorised - must contain a payload with the error
                                // message
                            if (hdr.payloadSize == 0) {
                                std::ostringstream ss;
                                ss << "Received " << hdr.message << " message with no payload from server";
                                pp->second.set_exception(
                                    std::make_exception_ptr(RemoteFDBException(ss.str(), controlEndpoint_)));
                            }
                            else {
                                std::string msg(hdr.payloadSize, ' ');
                                payload.copy(msg.data(), payload.size());

                                if (hdr.message == Message::Error) {
                                    pp->second.set_exception(
                                        std::make_exception_ptr(RemoteFDBException(msg, controlEndpoint_)));
                                }
                                else if (hdr.message == Message::Unauthorised) {
                                    pp->second.set_exception(
                                        std::make_exception_ptr(RemoteFDBUnauthorised(msg, controlEndpoint_)));
                                }
                                else {
                                    std::ostringstream ss;
                                    ss << "Received unexpected message " << hdr.message << " from server";
                                    pp->second.set_exception(
                                        std::make_exception_ptr(RemoteFDBException(ss.str(), controlEndpoint_)));
                                }
                            }
                        }
                        promises_.erase(pp);
                        handled = true;
                    }
                }
                if (!handled) {  // not the answer to a blocking request (a promise), so dispatch to the client
                    // Hold clientsMutex_ across the virtual dispatch too (not just the lookup)
                    std::lock_guard lock(clientsMutex_);

                    auto it = clients_.find(hdr.clientID());
                    if (it == clients_.end()) {
                        std::ostringstream ss;
                        ss << "ERROR: " << msgHeader(hdr, controlEndpoint_) << " - ClientID not found. ABORTING";
                        Log::status() << ss.str() << std::endl;
                        Log::error() << "Retrieving... " << ss.str() << std::endl;
                        throw SeriousBug(ss.str(), Here());
                    }
                    Client* client = it->second;

                    if (hdr.payloadSize == 0) {
                        handled = client->handle(hdr.message, hdr.requestID);
                    }
                    else {
                        handled = client->handle(hdr.message, hdr.requestID, std::move(payload));
                    }
                }

                if (!handled) {
                    std::ostringstream ss;
                    ss << "ERROR: " << msgHeader(hdr, controlEndpoint_);
                    if (hdr.message == Message::Error) {
                        ss << " - received an unhandled error";
                        if (hdr.payloadSize) {
                            std::string msg;
                            msg.resize(payload.size(), ' ');
                            payload.copy(msg.data(), payload.size());
                            ss << ": " << msg;
                        }
                        throw RemoteFDBException(ss.str(), controlEndpoint_);
                    }
                    ss << " - received unexpected message. ABORTING";
                    Log::status() << ss.str() << std::endl;
                    Log::error() << "Client Retrieving... " << ss.str() << std::endl;
                    throw SeriousBug(ss.str(), Here());
                }
            }
        }

        // We don't want to let exceptions escape inside a worker thread.
    }
    catch (...) {
        handleConnectionError(std::current_exception());
    }
}

void ClientConnection::closeConnection() {
    LOG_DEBUG_LIB(LibFdb5) << "ClientConnection::closeConnection() -- Data thread stopping" << std::endl;
    std::lock_guard lock(clientsMutex_);
    for (auto& [id, client] : clients_) {
        client->closeConnection();
    }
}

void ClientConnection::listeningDataThreadLoop() {

    try {

        LOG_DEBUG_LIB(LibFdb5) << "ClientConnection::listeningDataThreadLoop started" << std::endl;

        MessageHeader hdr;

        while (true) {

            Buffer payload = Connection::readData(hdr);

            LOG_DEBUG_LIB(LibFdb5) << "ClientConnection::listeningDataThreadLoop - " << msgHeader(hdr, dataEndpoint_)
                                   << std::endl;

            if (hdr.message == Message::Exit) {
                closeConnection();
                return;
            }
            if (hdr.clientID()) {
                bool handled = false;
                // Hold clientsMutex_ across the virtual dispatch too (not just the lookup)
                std::lock_guard lock(clientsMutex_);

                auto it = clients_.find(hdr.clientID());
                if (it == clients_.end()) {
                    std::ostringstream ss;
                    ss << "ERROR: " << msgHeader(hdr, dataEndpoint_) << " - ClientID not found. ABORTING";
                    Log::status() << ss.str() << std::endl;
                    Log::error() << "Retrieving... " << ss.str() << std::endl;
                    throw SeriousBug(ss.str(), Here());
                }
                Client* client = it->second;

                ASSERT(client);
                ASSERT(!hdr.control());
                if (hdr.payloadSize == 0) {
                    handled = client->handle(hdr.message, hdr.requestID);
                }
                else {
                    handled = client->handle(hdr.message, hdr.requestID, std::move(payload));
                }

                if (!handled) {
                    std::ostringstream ss;
                    ss << "ERROR: " << msgHeader(hdr, dataEndpoint_);
                    if (hdr.message == Message::Error) {
                        ss << " - received an unhandled error";
                        if (hdr.payloadSize) {
                            std::string msg;
                            msg.resize(payload.size(), ' ');
                            payload.copy(msg.data(), payload.size());
                            ss << ": " << msg;
                        }
                        throw RemoteFDBException(ss.str(), dataEndpoint_);
                    }
                    ss << " - received unexpected message. ABORTING";
                    Log::status() << ss.str() << std::endl;
                    Log::error() << "Client Retrieving... " << ss.str() << std::endl;
                    throw SeriousBug(ss.str(), Here());
                }
            }
        }

        // We don't want to let exceptions escape inside a worker thread.
    }
    catch (...) {
        handleConnectionError(std::current_exception());
    }
}

}  // namespace fdb5::remote
