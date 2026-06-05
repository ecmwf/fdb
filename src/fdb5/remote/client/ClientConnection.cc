
#include "fdb5/remote/client/ClientConnection.h"

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
#include <vector>

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

#include "fdb5/LibFdb5.h"
#include "fdb5/remote/Connection.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/remote/RemoteConfiguration.h"
#include "fdb5/remote/client/Client.h"
#include "fdb5/remote/client/ClientConnectionRouter.h"

using namespace eckit;
using namespace eckit::literals;

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

class DataWriteRequest {

public:

    DataWriteRequest() : client_(nullptr), msg_(Message::None), id_(0), data_(Buffer(0)) {}

    DataWriteRequest(Client* client, Message msg, uint32_t id, Buffer&& data) :
        client_(client), msg_(msg), id_(id), data_(std::move(data)) {}

    Client* client_;
    Message msg_;
    uint32_t id_;
    Buffer data_;
};


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
                Connection::write(Message::Stop, true, clientID, 0);
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
    if (dataWriteQueue_) {
        dataWriteQueue_->close();
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
    std::future<Buffer> f;
    {
        std::lock_guard<std::mutex> lock(promisesMutex_);
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
        std::lock_guard<std::mutex> lock(dataWriteMutex_);
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
    tracingEnabled_ = serverFunctionality.has("TracingEnabled");

    if (single_ && !(dataEndpoint_ == controlEndpoint_)) {
        Log::warning() << "Returned control interface does not match. " << dataEndpoint_ << " /= " << controlEndpoint_
                       << std::endl;
    }

    return serverSession;
}

std::string msgHeader(MessageHeader& hdr, net::Endpoint& endpoint) {
    std::ostringstream ss;
    ss << (hdr.control() ? "CONTROL" : "DATA") << " connection=" << endpoint << " [message=" << hdr.message
       << ",clientID=" << hdr.clientID() << ",requestID=" << hdr.requestID << ",payload=" << hdr.payloadSize << "]";
    return ss.str();
}

void ClientConnection::listeningControlThreadLoop() {

    try {

        MessageHeader hdr;

        while (true) {

            Buffer payload = Connection::readControl(hdr);
            LOG_DEBUG_LIB(LibFdb5) << "ClientConnection::listeningControlThreadLoop - "
                                   << msgHeader(hdr, controlEndpoint_) << std::endl;

            if (hdr.message == Message::Exit) {
                LOG_DEBUG_LIB(LibFdb5) << "CONTROL connection=" << controlEndpoint_ << " - thread stopping"
                                       << std::endl;
                return;
            }

            if (hdr.clientID()) {
                ASSERT(hdr.control() || single_);

                bool found = false;
                bool handled = false;
                {
                    // is the message a response to a blocking request?
                    // acquire the mutex and look for the request ID in the promises map
                    // only hold the mutex for the promise lookup/fulfillment, then release before calling handle().
                    std::lock_guard<std::mutex> lock(promisesMutex_);
                    auto pp = promises_.find(hdr.requestID);
                    if (pp != promises_.end()) {
                        found = true;
                        if (hdr.message == Message::Error) {  // this is an error response to a blocking request,
                                                              // set the exception on the promise
                            std::string errmsg =
                                (hdr.payloadSize == 0)
                                    ? "remote error - no error message provided"
                                    : std::string{static_cast<const char*>(payload.data()), payload.size()};
                            try {
                                pp->second.set_exception(
                                    std::make_exception_ptr(RemoteFDBException(errmsg, controlEndpoint())));
                            }
                            catch (const std::exception& e) {
                                Log::error()
                                    << "ERROR: " << msgHeader(hdr, controlEndpoint_) << " - received error \"" << errmsg
                                    << "\" for blocking request - unable to set the exception on the promise: "
                                    << e.what() << std::endl;
                            }
                        }
                        else {
                            if (hdr.payloadSize == 0) {
                                ASSERT(hdr.message == Message::Received);
                                pp->second.set_value(Buffer(0));
                            }
                            else {
                                pp->second.set_value(std::move(payload));
                            }
                            handled = true;
                        }
                        promises_.erase(pp);
                    }
                }
                if (!found) {
                    // if not a response to a blocking request, then it must be a message for a client, look up the
                    // client and call handle()
                    std::lock_guard<std::mutex> lock(clientsMutex_);
                    auto it = clients_.find(hdr.clientID());
                    if (it == clients_.end()) {
                        std::ostringstream ss;
                        ss << "ERROR: " << msgHeader(hdr, controlEndpoint_) << " - ClientID not found. ABORTING";
                        Log::status() << ss.str() << std::endl;
                        Log::error() << ss.str() << std::endl;
                        throw SeriousBug(ss.str(), Here());
                    }

                    auto* client = it->second;
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
                            std::string errmsg{static_cast<const char*>(payload.data()), payload.size()};
                            ss << ": \"" << errmsg << "\"";
                        }
                        Log::status() << ss.str() << std::endl;
                        Log::error() << ss.str() << std::endl;
                        throw RemoteFDBException(ss.str(), controlEndpoint_);
                    }
                    else {
                        ss << " - received unexpected message. ABORTING";
                        Log::status() << ss.str() << std::endl;
                        Log::error() << ss.str() << std::endl;
                        throw SeriousBug(ss.str(), Here());
                    }
                }
            }
        }
        // We don't want to let exceptions escape inside a worker thread.
    }
    catch (const std::exception& e) {
        ClientConnectionRouter::instance().teardown(std::make_exception_ptr(e));
    }
    catch (...) {
        ClientConnectionRouter::instance().teardown(std::current_exception());
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
                LOG_DEBUG_LIB(LibFdb5) << "DATA connection=" << dataEndpoint_ << " - thread stopping" << std::endl;
                std::lock_guard<std::mutex> lock(clientsMutex_);
                for (auto& [id, client] : clients_) {
                    client->closeConnection();
                }
                return;
            }

            if (hdr.clientID()) {
                ASSERT(!hdr.control());

                bool handled = false;

                // Hold clientsMutex_ across handle() to prevent the Client
                // from being destroyed (via remove()) while handle() is in flight.
                std::lock_guard<std::mutex> lock(clientsMutex_);

                auto it = clients_.find(hdr.clientID());
                if (it == clients_.end()) {
                    std::ostringstream ss;
                    ss << "ERROR: " << msgHeader(hdr, dataEndpoint_) << " - ClientID not found. ABORTING";
                    Log::status() << ss.str() << std::endl;
                    Log::error() << ss.str() << std::endl;
                    throw SeriousBug(ss.str(), Here());
                }

                auto* client = it->second;
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
                            std::string errmsg{static_cast<const char*>(payload.data()), payload.size()};
                            ss << ": \"" << errmsg << "\"";
                        }
                        Log::status() << ss.str() << std::endl;
                        Log::error() << ss.str() << std::endl;
                        throw RemoteFDBException(ss.str(), dataEndpoint_);
                    }
                    else {
                        ss << " - received unexpected message. ABORTING";
                        Log::status() << ss.str() << std::endl;
                        Log::error() << ss.str() << std::endl;
                        throw SeriousBug(ss.str(), Here());
                    }
                }
            }
        }

        // We don't want to let exceptions escape inside a worker thread.
    }
    catch (const std::exception& e) {
        ClientConnectionRouter::instance().teardown(std::make_exception_ptr(e));
    }
    catch (...) {
        ClientConnectionRouter::instance().teardown(std::current_exception());
    }
}

}  // namespace fdb5::remote
