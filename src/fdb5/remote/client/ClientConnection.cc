

#include <functional>
#include <unistd.h>

#include "eckit/config/LocalConfiguration.h"
#include "eckit/config/Resource.h"
#include "eckit/io/Buffer.h"
#include "eckit/log/Bytes.h"
#include "eckit/log/Log.h"
#include "eckit/message/Message.h"
#include "eckit/os/BackTrace.h"
#include "eckit/runtime/Main.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/utils/Translator.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/remote/RemoteFieldLocation.h"
#include "fdb5/remote/client/ClientConnection.h"
#include "fdb5/remote/client/ClientConnectionRouter.h"

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

namespace{

class TCPException : public eckit::Exception {
public:
    TCPException(const std::string& msg, const eckit::CodeLocation& here) :
        eckit::Exception(std::string("TCPException: ") + msg, here) {

        eckit::Log::error() << "TCP Exception; backtrace(): " << std::endl;
        eckit::Log::error() << eckit::BackTrace::dump() << std::endl;
    }
};

}

//----------------------------------------------------------------------------------------------------------------------

class DataWriteRequest {

public:

    DataWriteRequest() :
        client_(nullptr), msg_(Message::None), id_(0), data_(eckit::Buffer(0)) {}

    DataWriteRequest(Client* client, Message msg, uint32_t id, eckit::Buffer&& data) :
        client_(client), msg_(msg), id_(id), data_(std::move(data)) {}

    Client* client_;
    Message msg_;
    uint32_t id_;
    eckit::Buffer data_;
};


ClientConnection::ClientConnection(const eckit::net::Endpoint& controlEndpoint, const std::string& defaultEndpoint):
    controlEndpoint_(controlEndpoint), defaultEndpoint_(defaultEndpoint), id_(1), connected_(false), dataWriteQueue_(nullptr) {

    eckit::Log::debug<LibFdb5>() << "ClientConnection::ClientConnection() controlEndpoint: " << controlEndpoint << std::endl;
}

void ClientConnection::add(Client& client) {
    std::lock_guard<std::mutex> lock(clientsMutex_);
    auto it = clients_.find(client.id());
    ASSERT(it == clients_.end());

    clients_[client.id()] = &client;
}

bool ClientConnection::remove(uint32_t clientID) {
    if (clientID > 0) {

        //std::lock_guard<std::mutex> lock(clientsMutex_);
        auto it = clients_.find(clientID);

        if (it != clients_.end()) {
            Connection::write(Message::Exit, true, clientID, 0);
            // TODO make the data connection dying automatically, when there are no more async writes
            Connection::write(Message::Exit, false, clientID, 0);

            clients_.erase(it);
        }
    }

    if (clients_.empty()) {
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
    ASSERT(id_ < UINT32_MAX-2);
    return ++id_;
}

bool ClientConnection::connect(bool singleAttempt) {

//    std::lock_guard<std::mutex> lock(requestMutex_);
    if (connected_) {
        eckit::Log::warning() << "ClientConnection::connect() called when already connected" << std::endl;
        return connected_;
    }

    int fdbMaxConnectRetries = (singleAttempt ? 1 : eckit::Resource<int>("fdbMaxConnectRetries", 3));
    int fdbConnectTimeout = eckit::Resource<int>("fdbConnectTimeout", (singleAttempt ? 2 : 5)); // 0 = No timeout 

    try {
        // Connect to server, and check that the server is happy on the response 
        eckit::Log::debug<LibFdb5>() << "Connecting to host: " << controlEndpoint_ << std::endl;
        controlClient_.connect(controlEndpoint_, fdbMaxConnectRetries, fdbConnectTimeout);

        writeControlStartupMessage();
        eckit::SessionID serverSession = verifyServerStartupResponse();

        // Connect to the specified data port
        eckit::Log::debug<LibFdb5>() << "Received data endpoint from host: " << dataEndpoint_ << std::endl;
        dataClient_.connect(dataEndpoint_, fdbMaxConnectRetries, fdbConnectTimeout);
        writeDataStartupMessage(serverSession);

        // And the connections are set up. Let everything start up!
        listeningThread_ = std::thread([this] { listeningThreadLoop(); });

        connected_ = true;
        return true;
    } catch(eckit::TooManyRetries& e) {
        if (controlClient_.isConnected()) {
            controlClient_.close();
        }
    }
    return false;
}

void ClientConnection::disconnect() {

    ASSERT(clients_.empty());
    if (connected_) {

        listeningThread_.join();

        // Close both the control and data connections
        controlClient_.close();
        dataClient_.close();
        connected_ = false;
    }
}

const eckit::net::Endpoint& ClientConnection::controlEndpoint() const {
    return controlEndpoint_;
}

eckit::LocalConfiguration ClientConnection::availableFunctionality() const {
    eckit::LocalConfiguration conf;
    std::vector<int> remoteFieldLocationVersions = {1};
    conf.set("RemoteFieldLocation", remoteFieldLocationVersions);
    return conf;
}

// -----------------------------------------------------------------------------------------------------

void ClientConnection::controlWrite(Client& client, Message msg, uint32_t requestID, bool dataListener, std::vector<std::pair<const void*, uint32_t>> data) {
    auto it = clients_.find(client.clientId());
    ASSERT(it != clients_.end());

    Connection::write(msg, true, client.clientId(), requestID, data);
}

void ClientConnection::dataWrite(DataWriteRequest& r) {
    Connection::write(r.msg_, false, r.client_->clientId(), r.id_, r.data_.data(), r.data_.size());
}

void ClientConnection::dataWrite(Client& client, remote::Message msg, uint32_t requestID, std::vector<std::pair<const void*, uint32_t>> data) {

    static size_t maxQueueLength = eckit::Resource<size_t>("fdbDataWriteQueueLength;$FDB_DATA_WRITE_QUEUE_LENGTH", 200);
    auto it = clients_.find(client.clientId());
    ASSERT(it != clients_.end());

    if (!dataWriteFuture_.valid()) {

        {
            // Reset the queue after previous done/errors
            std::lock_guard<std::mutex> lock(dataWriteQueueMutex_);
            ASSERT(!dataWriteQueue_);

            dataWriteQueue_.reset(new eckit::Queue<DataWriteRequest>{maxQueueLength});
        }

        dataWriteFuture_ = std::async(std::launch::async, [this] { return dataWriteThreadLoop(); });
    }

    uint32_t payloadLength = 0;
    for (auto d: data) {
        ASSERT(d.first);
        payloadLength += d.second;
    }

    eckit::Buffer buffer{payloadLength};
    uint32_t offset = 0;
    for (auto d: data) {
        buffer.copy(d.first, d.second, offset);
        offset += d.second;
    }

    dataWriteQueue_->emplace(&client, msg, requestID, std::move(buffer));
//    Connection::write(msg, false, client.clientId(), requestID, data);
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

    } catch (...) {
        dataWriteQueue_->interrupt(std::current_exception());
        throw;
    }

    // We are inside an async, so don't need to worry about exceptions escaping.
    // They will be released when flush() is called.
}

void ClientConnection::handleError(const MessageHeader& hdr, eckit::Buffer buffer) {
    ASSERT(hdr.marker == StartMarker);
    ASSERT(hdr.version == CurrentVersion);

    if (hdr.message == Message::Error) {
        ASSERT(hdr.payloadSize > 9);
        std::string what(buffer.size()+1, ' ');
        buffer.copy(what.c_str(), buffer.size());
        what[buffer.size()] = 0; // Just in case

        throw RemoteFDBException(what, controlEndpoint_);
    }
}


void ClientConnection::writeControlStartupMessage() {

    eckit::Buffer payload(4096);
    eckit::MemoryStream s(payload);
    s << sessionID_;
    s << eckit::net::Endpoint(controlEndpoint_.hostname(), controlEndpoint_.port());
    s << LibFdb5::instance().remoteProtocolVersion().used();

    // TODO: Abstract this dictionary into a RemoteConfiguration object, which
    //       understands how to do the negotiation, etc, but uses Value (i.e.
    //       essentially JSON) over the wire for flexibility.
    s << availableFunctionality().get();

    // controlWrite(Message::Startup, 0, 0, std::vector<std::pair<const void*, uint32_t>>{{payload, s.position()}});
    Connection::write(Message::Startup, true, 0, 0, payload, s.position());
}

void ClientConnection::writeDataStartupMessage(const eckit::SessionID& serverSession) {

    eckit::Buffer payload(1024);
    eckit::MemoryStream s(payload);

    s << sessionID_;
    s << serverSession;

    // dataWrite(Message::Startup, 0, 0, std::vector<std::pair<const void*, uint32_t>>{{payload, s.position()}});
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

    if (dataEndpoint_.hostname() != controlEndpoint_.hostname()) {
        eckit::Log::warning() << "Data and control interface hostnames do not match. "
                       << dataEndpoint_.hostname() << " /= "
                       << controlEndpoint_.hostname() << std::endl;
    }

    if (clientSession != sessionID_) {
        std::stringstream ss;
        ss << "Session ID does not match session received from server: "
           << sessionID_ << " != " << clientSession;
        throw eckit::BadValue(ss.str(), Here());
    }
    if (serverFunctionality.has("NumberOfConnections") && serverFunctionality.getInt("NumberOfConnections")==1) {
        single_ = true;
    }

    if (single_ && !(dataEndpoint_ == controlEndpoint_)) {
        eckit::Log::warning() << "Returned control interface does not match. "
                       << dataEndpoint_ << " /= "
                       << controlEndpoint_ << std::endl;
    }

    return serverSession;
}

void ClientConnection::listeningThreadLoop() {

    try {

        MessageHeader hdr;
        eckit::FixedString<4> tail;

        while (true) {

            eckit::Buffer payload = Connection::readData(hdr);

            eckit::Log::debug<LibFdb5>() << "ClientConnection::listeningThreadLoop - got [message=" << hdr.message << ",requestID=" << hdr.requestID << ",payload=" << hdr.payloadSize << "]" << std::endl;

            if (hdr.message == Message::Exit) {

                if (clients_.empty()) {
                    return;
                }
            } else {
                if (hdr.clientID()) {
                    bool handled = false;
                    auto it = clients_.find(hdr.clientID());
                    if (it == clients_.end()) {
                        std::stringstream ss;
                        ss << "ERROR: Received [clientID="<< hdr.clientID() << ",requestID="<< hdr.requestID << ",message=" << hdr.message << ",payload=" << hdr.payloadSize << "]" << std::endl;
                        ss << "Unexpected answer for clientID recieved (" << hdr.clientID() << "). ABORTING";
                        eckit::Log::status() << ss.str() << std::endl;
                        eckit::Log::error() << "Retrieving... " << ss.str() << std::endl;
                        throw eckit::SeriousBug(ss.str(), Here());

                        ASSERT(false); // todo report the error
                    }

                    if (hdr.payloadSize == 0) {
                        if (it->second->blockingRequestId() == hdr.requestID) {
                            ASSERT(hdr.message == Message::Received);
                            handled = it->second->response(hdr.requestID);
                        } else {
                            handled = it->second->handle(hdr.message, hdr.control(), hdr.requestID);
                        }
                    }
                    else {
                        if (it->second->blockingRequestId() == hdr.requestID) {
                            handled = it->second->response(hdr.requestID, std::move(payload));
                        } else {
                            handled = it->second->handle(hdr.message, hdr.control(), hdr.requestID, std::move(payload));
                        }
                    }

                    if (!handled) {
                        std::stringstream ss;
                        ss << "ERROR: Unexpected message recieved (" << hdr.message << "). ABORTING";
                        eckit::Log::status() << ss.str() << std::endl;
                        eckit::Log::error() << "Client Retrieving... " << ss.str() << std::endl;
                        throw eckit::SeriousBug(ss.str(), Here());
                    }
                }
            }
        }

    // We don't want to let exceptions escape inside a worker thread.
    } catch (const std::exception& e) {
//        ClientConnectionRouter::instance().handleException(std::make_exception_ptr(e));
    } catch (...) {
//        ClientConnectionRouter::instance().handleException(std::current_exception());
    }
    // ClientConnectionRouter::instance().deregister(*this);
}

}  // namespace fdb5::remote
