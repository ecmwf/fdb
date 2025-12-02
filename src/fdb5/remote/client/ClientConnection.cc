
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

using namespace eckit::literals;

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

class DataWriteRequest {

public:

    DataWriteRequest() : client_(nullptr), msg_(Message::None), id_(0), data_(eckit::Buffer(0)) {}

    DataWriteRequest(Client* client, Message msg, uint32_t id, eckit::Buffer&& data) :
        client_(client), msg_(msg), id_(id), data_(std::move(data)) {}

    Client* client_;
    Message msg_;
    uint32_t id_;
    eckit::Buffer data_;
};


ClientConnection::ClientConnection(const eckit::net::Endpoint& controlEndpoint, const std::string& defaultEndpoint) :
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

bool ClientConnection::connect(const eckit::Configuration& config, bool singleAttempt) {

    if (connected_) {
        eckit::Log::warning() << "ClientConnection::connect() called when already connected" << std::endl;
        return connected_;
    }

    int fdbMaxConnectRetries = (singleAttempt ? 1 : eckit::Resource<int>("fdbMaxConnectRetries", 3));
    int fdbConnectTimeout = eckit::Resource<int>("fdbConnectTimeout", (singleAttempt ? 2 : 5));  // 0 = No timeout

    try {
        // Connect to server, and check that the server is happy on the response
        LOG_DEBUG_LIB(LibFdb5) << "Connecting to host: " << controlEndpoint_ << std::endl;
        controlClient_.connect(controlEndpoint_, fdbMaxConnectRetries, fdbConnectTimeout);

        writeControlStartupMessage(config);
        eckit::SessionID serverSession = verifyServerStartupResponse();

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
    catch (eckit::TooManyRetries& e) {
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

const eckit::net::Endpoint& ClientConnection::controlEndpoint() const {
    return controlEndpoint_;
}

RemoteConfiguration ClientConnection::availableFunctionality(const eckit::Configuration& config) const {
    return RemoteConfiguration{config};
}

//----------------------------------------------------------------------------------------------------------------------

std::future<eckit::Buffer> ClientConnection::controlWrite(const Client& client, const Message msg,
                                                          const uint32_t requestID, const bool /*dataListener*/,
                                                          const PayloadList payloads) const {
    std::future<eckit::Buffer> f;
    {
        std::lock_guard<std::mutex> lock(promisesMutex_);
        auto pp = promises_.emplace(requestID, std::promise<eckit::Buffer>{}).first;
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

    static size_t maxQueueLength = eckit::Resource<size_t>("fdbDataWriteQueueLength;$FDB_DATA_WRITE_QUEUE_LENGTH", 320);

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

            dataWriteQueue_ = std::make_unique<eckit::Queue<DataWriteRequest>>(maxQueueLength);
            dataWriteThread_ = std::thread([this] { dataWriteThreadLoop(); });
        }
    }

    uint32_t payloadLength = 0;
    for (const auto& payload : payloads) {
        ASSERT(payload.data);
        payloadLength += payload.length;
    }

    eckit::Buffer buffer{payloadLength};
    uint32_t offset = 0;
    for (const auto& payload : payloads) {
        buffer.copy(payload.data, payload.length, offset);
        offset += payload.length;
    }

    dataWriteQueue_->emplace(&client, msg, requestID, std::move(buffer));
}

void ClientConnection::dataWriteThreadLoop() {

    eckit::Timer timer;
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

void ClientConnection::writeControlStartupMessage(const eckit::Configuration& config) {

    eckit::Buffer payload(4096);
    eckit::MemoryStream s(payload);
    s << sessionID_;
    s << eckit::net::Endpoint(controlEndpoint_.hostname(), controlEndpoint_.port());
    s << LibFdb5::instance().remoteProtocolVersion().used();
    s << availableFunctionality(config);

    LOG_DEBUG_LIB(LibFdb5) << "writeControlStartupMessage - Sending session " << sessionID_ << " to control "
                           << controlEndpoint_ << std::endl;
    Connection::write(Message::Startup, true, 0, 0, payload, s.position());
}

void ClientConnection::writeDataStartupMessage(const eckit::SessionID& serverSession) {

    eckit::Buffer payload(1_KiB);
    eckit::MemoryStream s(payload);

    s << sessionID_;
    s << serverSession;

    LOG_DEBUG_LIB(LibFdb5) << "writeDataStartupMessage - Sending session " << sessionID_ << " to data " << dataEndpoint_
                           << std::endl;
    Connection::write(Message::Startup, false, 0, 0, payload, s.position());
}

eckit::SessionID ClientConnection::verifyServerStartupResponse() {

    MessageHeader hdr;
    eckit::Buffer payload = Connection::readControl(hdr);

    ASSERT(hdr.requestID == 0);

    eckit::MemoryStream s(payload);
    eckit::SessionID clientSession(s);
    eckit::SessionID serverSession(s);
    eckit::net::Endpoint dataEndpoint(s);
    eckit::LocalConfiguration serverFunctionality(s);

    dataEndpoint_ = dataEndpoint;

    LOG_DEBUG_LIB(LibFdb5) << "verifyServerStartupResponse - Received from server " << clientSession << " "
                           << serverSession << " " << dataEndpoint << std::endl;
    if (dataEndpoint_.hostname() != controlEndpoint_.hostname()) {
        eckit::Log::warning() << "Data and control interface hostnames do not match. " << dataEndpoint_.hostname()
                              << " /= " << controlEndpoint_.hostname() << std::endl;
    }

    if (clientSession != sessionID_) {
        std::stringstream ss;
        ss << "Session ID does not match session received from server: " << sessionID_ << " != " << clientSession;
        throw eckit::BadValue(ss.str(), Here());
    }
    if (serverFunctionality.has("NumberOfConnections") && serverFunctionality.getInt("NumberOfConnections") == 1) {
        single_ = true;
    }

    if (single_ && !(dataEndpoint_ == controlEndpoint_)) {
        eckit::Log::warning() << "Returned control interface does not match. " << dataEndpoint_
                              << " /= " << controlEndpoint_ << std::endl;
    }

    return serverSession;
}

void ClientConnection::listeningControlThreadLoop() {

    try {

        MessageHeader hdr;

        while (true) {

            eckit::Buffer payload = Connection::readControl(hdr);

            LOG_DEBUG_LIB(LibFdb5) << "ClientConnection::listeningControlThreadLoop - got [message=" << hdr.message
                                   << ",clientID=" << hdr.clientID() << ",control=" << hdr.control()
                                   << ",requestID=" << hdr.requestID << ",payload=" << hdr.payloadSize << "]"
                                   << std::endl;

            if (hdr.message == Message::Exit) {
                LOG_DEBUG_LIB(LibFdb5) << "ClientConnection::listeningControlThreadLoop() -- Control thread stopping"
                                       << std::endl;
                return;
            }
            else {
                if (hdr.clientID()) {
                    bool handled = false;

                    ASSERT(hdr.control() || single_);

                    std::lock_guard<std::mutex> lock(promisesMutex_);

                    auto pp = promises_.find(hdr.requestID);
                    if (pp != promises_.end()) {
                        if (hdr.payloadSize == 0) {
                            ASSERT(hdr.message == Message::Received);
                            pp->second.set_value(eckit::Buffer(0));
                        }
                        else {
                            pp->second.set_value(std::move(payload));
                        }
                        promises_.erase(pp);
                        handled = true;
                    }
                    else {
                        Client* client = nullptr;
                        {
                            std::lock_guard lock(clientsMutex_);

                            auto it = clients_.find(hdr.clientID());
                            if (it == clients_.end()) {
                                std::stringstream ss;
                                ss << "ERROR: CONTROL connection=" << controlEndpoint_
                                   << " received [clientID=" << hdr.clientID() << ",requestID=" << hdr.requestID
                                   << ",message=" << hdr.message << ",payload=" << hdr.payloadSize << "]" << std::endl;
                                ss << "ClientID (" << hdr.clientID() << ") not found. ABORTING";
                                eckit::Log::status() << ss.str() << std::endl;
                                eckit::Log::error() << "Retrieving... " << ss.str() << std::endl;
                                throw eckit::SeriousBug(ss.str(), Here());
                            }
                            client = it->second;
                        }

                        if (hdr.payloadSize == 0) {
                            handled = client->handle(hdr.message, hdr.requestID);
                        }
                        else {
                            handled = client->handle(hdr.message, hdr.requestID, std::move(payload));
                        }
                    }

                    if (!handled) {
                        std::stringstream ss;
                        ss << "ERROR: CONTROL connection=" << controlEndpoint_
                           << "Unexpected message recieved [message=" << hdr.message << ",clientID=" << hdr.clientID()
                           << ",requestID=" << hdr.requestID << "]. ABORTING";
                        eckit::Log::status() << ss.str() << std::endl;
                        eckit::Log::error() << "Client Retrieving... " << ss.str() << std::endl;
                        throw eckit::SeriousBug(ss.str(), Here());
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

            eckit::Buffer payload = Connection::readData(hdr);

            LOG_DEBUG_LIB(LibFdb5) << "ClientConnection::listeningDataThreadLoop - got [message=" << hdr.message
                                   << ",requestID=" << hdr.requestID << ",payload=" << hdr.payloadSize << "]"
                                   << std::endl;

            if (hdr.message == Message::Exit) {
                closeConnection();
                return;
            }
            else {
                if (hdr.clientID()) {
                    bool handled = false;
                    Client* client = nullptr;
                    {
                        std::lock_guard lock(clientsMutex_);

                        auto it = clients_.find(hdr.clientID());
                        if (it == clients_.end()) {
                            std::stringstream ss;
                            ss << "ERROR: DATA connection=" << dataEndpoint_ << " received [clientID=" << hdr.clientID()
                               << ",requestID=" << hdr.requestID << ",message=" << hdr.message
                               << ",payload=" << hdr.payloadSize << "]" << std::endl;
                            ss << "ClientID (" << hdr.clientID() << ") not found. ABORTING";
                            eckit::Log::status() << ss.str() << std::endl;
                            eckit::Log::error() << "Retrieving... " << ss.str() << std::endl;
                            throw eckit::SeriousBug(ss.str(), Here());
                        }
                        client = it->second;
                    }

                    ASSERT(client);
                    ASSERT(!hdr.control());
                    if (hdr.payloadSize == 0) {
                        handled = client->handle(hdr.message, hdr.requestID);
                    }
                    else {
                        handled = client->handle(hdr.message, hdr.requestID, std::move(payload));
                    }

                    if (!handled) {
                        std::stringstream ss;
                        ss << "ERROR: DATA connection=" << dataEndpoint_ << " Unexpected message recieved ("
                           << hdr.message << "). ABORTING";
                        eckit::Log::status() << ss.str() << std::endl;
                        eckit::Log::error() << "Client Retrieving... " << ss.str() << std::endl;
                        throw eckit::SeriousBug(ss.str(), Here());
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
