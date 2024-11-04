

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

        std::cerr << "TCP Exception; backtrace(): " << std::endl;
        std::cerr << eckit::BackTrace::dump() << std::endl;
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
    controlEndpoint_(controlEndpoint), defaultEndpoint_(defaultEndpoint), id_(1), connected_(false), controlStopping_(false), dataStopping_(false), dataWriteQueue_(nullptr) {

    std::cerr << "ClientConnection::ClientConnection() controlEndpoint: " << controlEndpoint << std::endl;
}

void ClientConnection::add(Client& client) {
    std::lock_guard<std::mutex> lock(clientsMutex_);
    // auto it = clients_.find(client.id());
    // ASSERT(it == clients_.end());

    clients_[client.id()] = &client;
}

bool ClientConnection::remove(uint32_t clientID) {

    std::lock_guard<std::mutex> lock(clientsMutex_);

    if (clientID > 0) {

        auto it = clients_.find(clientID);

        if (it != clients_.end()) {
            Connection::write(Message::Stop, true, clientID, 0);

            clients_.erase(it);
        }
    }

    if (clients_.empty()) {
        teardown();
        // ClientConnectionRouter::instance().deregister(*this);
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

std::mutex countMutex;
static int connectCount = 0;

bool ClientConnection::connect(bool singleAttempt) {

    {
	    std::lock_guard<std::mutex> lock(countMutex);
	    connectCount++;
	    std::cerr << "ClientConnection::connect() count - " << connectCount << std::endl;
    }


//    std::lock_guard<std::mutex> lock(requestMutex_);
    if (connected_) {
        std::cerr << "ClientConnection::connect() called when already connected" << std::endl;
        return connected_;
    }

    int fdbMaxConnectRetries = (singleAttempt ? 1 : eckit::Resource<int>("fdbMaxConnectRetries", 3));
    int fdbConnectTimeout = eckit::Resource<int>("fdbConnectTimeout", (singleAttempt ? 2 : 5)); // 0 = No timeout 

    try {
        // Connect to server, and check that the server is happy on the response 
	    std::cerr << "Connecting to host: " << controlEndpoint_ << std::endl;
        controlClient_.connect(controlEndpoint_, fdbMaxConnectRetries, fdbConnectTimeout);

        writeControlStartupMessage();
        eckit::SessionID serverSession = verifyServerStartupResponse();

        if (!single_) {
            // Connect to the specified data port
		std::cerr << "Received data endpoint from host: " << dataEndpoint_ << std::endl;
            dataClient_.connect(dataEndpoint_, fdbMaxConnectRetries, fdbConnectTimeout);
            writeDataStartupMessage(serverSession);

            listeningDataThread_ = std::thread([this] { listeningDataThreadLoop(); });
        }

        listeningControlThread_ = std::thread([this] { listeningControlThreadLoop(); });

        connected_ = true;
    } catch(eckit::TooManyRetries& e) {
        if (controlClient_.isConnected()) {
            controlClient_.close();
        }
    }
    return connected_;
}

void ClientConnection::disconnect() {

    std::lock_guard<std::mutex> lock(clientsMutex_);
    ASSERT(clients_.empty());
    if (connected_) {

        // if (dataWriteFuture_.valid()) {
        //     dataWriteFuture_.wait();
        // }
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
    std::vector<int> numberOfConnections = {1,2};
    conf.set("NumberOfConnections", numberOfConnections);
    conf.set("PreferSingleConnection", false);
    return conf;
}

// -----------------------------------------------------------------------------------------------------

std::future<eckit::Buffer> ClientConnection::controlWrite(Client& client, Message msg, uint32_t requestID, bool dataListener, std::vector<std::pair<const void*, uint32_t>> data) {

    // std::lock_guard<std::mutex> lock(clientsMutex_);
    // auto it = clients_.find(client.clientId());
    // ASSERT(it != clients_.end());

    std::future<eckit::Buffer> f;
    {
        std::lock_guard<std::mutex> lock(promisesMutex_);
        auto pp = promises_.emplace(requestID, std::promise<eckit::Buffer>{}).first;
        f = pp->second.get_future();
    }
    Connection::write(msg, true, client.clientId(), requestID, data);

    return f;
}

void ClientConnection::dataWrite(DataWriteRequest& r) {
    // std::cout << "Write " << r.msg_ << ",clientID=" << r.client_->clientId() << ",requestID=" << r.id_ << std::endl;
    Connection::write(r.msg_, false, r.client_->clientId(), r.id_, r.data_.data(), r.data_.size());
}

void ClientConnection::dataWrite(Client& client, remote::Message msg, uint32_t requestID, std::vector<std::pair<const void*, uint32_t>> data) {

    static size_t maxQueueLength = eckit::Resource<size_t>("fdbDataWriteQueueLength;$FDB_DATA_WRITE_QUEUE_LENGTH", 320);
    {
        // retrieve or add client to the list
        std::lock_guard<std::mutex> lock(clientsMutex_);
        auto it = clients_.find(client.clientId());
        ASSERT(it != clients_.end());        
    }
    {
        std::lock_guard<std::mutex> lock(dataWriteMutex_);
        if (!dataWriteThread_.joinable()) {
            // Reset the queue after previous done/errors
            ASSERT(!dataWriteQueue_);

            dataWriteQueue_.reset(new eckit::Queue<DataWriteRequest>{maxQueueLength});
            dataWriteThread_ = std::thread([this] { dataWriteThreadLoop(); });
            // dataWriteFuture_ = std::async(std::launch::async, [this] { return dataWriteThreadLoop(); });
        }
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

    eckit::Log::info() << "writeControlStartupMessage - Sending session " << sessionID_ << " to control " << controlEndpoint_ << std::endl;
    Connection::write(Message::Startup, true, 0, 0, payload, s.position());
}

void ClientConnection::writeDataStartupMessage(const eckit::SessionID& serverSession) {

    eckit::Buffer payload(1024);
    eckit::MemoryStream s(payload);

    s << sessionID_;
    s << serverSession;

    eckit::Log::info() << "writeDataStartupMessage - Sending session " << sessionID_ << " to data " << dataEndpoint_ << std::endl;
    Connection::write(Message::Startup, false, 0, 0, payload, s.position());
}

eckit::SessionID ClientConnection::verifyServerStartupResponse() {

    MessageHeader hdr;
    eckit::Buffer payload = Connection::readControl(hdr);
    std::cerr << "Verify: 1 " << std::endl;

    ASSERT(hdr.requestID == 0);

    std::cerr << "Verify: 1a " << std::endl;
    eckit::MemoryStream s(payload);
    std::cerr << "Verify: 1b " << std::endl;
    eckit::SessionID clientSession(s);
    std::cerr << "Verify: 1c " << std::endl;
    eckit::SessionID serverSession(s);
    std::cerr << "Verify: 1d " << std::endl;
    eckit::net::Endpoint dataEndpoint(s);
    std::cerr << "Verify: 1e " << std::endl;
    eckit::LocalConfiguration serverFunctionality(s);
    std::cerr << "Verify: 2 " << std::endl;
    std::cerr << "Client session: " << clientSession << std::endl;
    std::cerr << "Server session: " << serverSession << std::endl;
    std::cerr << "Endpoint: " << dataEndpoint << std::endl;
    std::cerr << "Payloud: " << serverFunctionality << std::endl;

    dataEndpoint_ = dataEndpoint;

    eckit::Log::info() << "verifyServerStartupResponse - Received from server " << clientSession << " " << serverSession << " " << dataEndpoint << std::endl;

    if (dataEndpoint_.hostname() != controlEndpoint_.hostname()) {
        eckit::Log::warning() << "Data and control interface hostnames do not match. "
                       << dataEndpoint_.hostname() << " /= "
                       << controlEndpoint_.hostname() << std::endl;
    }
    std::cerr << "Verify: 3 " << std::endl;

    if (clientSession != sessionID_) {
        std::stringstream ss;
        ss << "Session ID does not match session received from server: "
           << sessionID_ << " != " << clientSession;
        throw eckit::BadValue(ss.str(), Here());
    }
    std::cerr << "Verify: 4 " << std::endl;
    if (serverFunctionality.has("NumberOfConnections") && serverFunctionality.getInt("NumberOfConnections")==1) {
        single_ = true;
    }
    std::cerr << "Verify: 5 " << std::endl;

    if (single_ && !(dataEndpoint_ == controlEndpoint_)) {
        eckit::Log::warning() << "Returned control interface does not match. "
                       << dataEndpoint_ << " /= "
                       << controlEndpoint_ << std::endl;
    }
    std::cerr << "Verify: 6 " << std::endl;

    return serverSession;
}

void ClientConnection::listeningControlThreadLoop() {

    try {

        MessageHeader hdr;

        while (true) {

            eckit::Buffer payload = Connection::readControl(hdr);

            std::cerr << "ClientConnection::listeningControlThreadLoop - got [message=" << hdr.message << ",clientID=" << hdr.clientID() << ",control=" << hdr.control() << ",requestID=" << hdr.requestID << ",payload=" << hdr.payloadSize << "]" << std::endl;

            if (hdr.message == Message::Exit) {
                controlStopping_ = true;
                return;
            } else {
                if (hdr.clientID()) {
                    bool handled = false;

                    ASSERT(hdr.control() || single_);

                    auto pp = promises_.find(hdr.requestID);
                    if (pp != promises_.end()) {
                        std::lock_guard<std::mutex> lock(promisesMutex_);
                        if (hdr.payloadSize == 0) {
                            ASSERT(hdr.message == Message::Received);
                            pp->second.set_value(eckit::Buffer(0));
                        } else {
                            pp->second.set_value(std::move(payload));
                        }
                        promises_.erase(pp);
                        handled = true;
                    } else {
                        Client* client = nullptr;
                        {
                            std::lock_guard<std::mutex> lock(clientsMutex_);

                            auto it = clients_.find(hdr.clientID());
                            if (it == clients_.end()) {
                                std::stringstream ss;
                                ss << "ERROR: connection=" << controlEndpoint_ << " received [clientID="<< hdr.clientID() << ",requestID="<< hdr.requestID << ",message=" << hdr.message << ",payload=" << hdr.payloadSize << "]" << std::endl;
                                ss << "Unexpected answer for clientID recieved (" << hdr.clientID() << "). ABORTING";
                                eckit::Log::status() << ss.str() << std::endl;
                                std::cerr << "Retrieving... " << ss.str() << std::endl;
                                throw eckit::SeriousBug(ss.str(), Here());
                            }
                            client = it->second;
                        }

                        if (hdr.payloadSize == 0) {
                            handled = client->handle(hdr.message, hdr.control(), hdr.requestID);
                        }
                        else {
                            handled = client->handle(hdr.message, hdr.control(), hdr.requestID, std::move(payload));
                        }
                    }

                    if (!handled) {
                        std::stringstream ss;
                        ss << "ERROR: connection=" << controlEndpoint_ << "Unexpected message recieved [message=" << hdr.message << ",clientID=" << hdr.clientID() << ",requestID=" << hdr.requestID << "]. ABORTING";
                        eckit::Log::status() << ss.str() << std::endl;
                        std::cerr << "Client Retrieving... " << ss.str() << std::endl;
                        throw eckit::SeriousBug(ss.str(), Here());
                    }
                }
            }
        }

    // We don't want to let exceptions escape inside a worker thread.
    } catch (const std::exception& e) {
       ClientConnectionRouter::instance().teardown(std::make_exception_ptr(e));
    } catch (...) {
       ClientConnectionRouter::instance().teardown(std::current_exception());
    }
    // ClientConnectionRouter::instance().deregister(*this);
}

void ClientConnection::listeningDataThreadLoop() {

    try {

        std::cerr << "ClientConnection::listeningDataThreadLoop started" << std::endl;

        MessageHeader hdr;

        while (true) {

            eckit::Buffer payload = Connection::readData(hdr);

            std::cerr << "ClientConnection::listeningDataThreadLoop - got [message=" << hdr.message << ",requestID=" << hdr.requestID << ",payload=" << hdr.payloadSize << "]" << std::endl;

            if (hdr.message == Message::Exit) {
                dataStopping_ = true;
                return;
            } else {
                if (hdr.clientID()) {
                    bool handled = false;
                    Client* client = nullptr;
                    {
                        std::lock_guard<std::mutex> lock(clientsMutex_);

                        auto it = clients_.find(hdr.clientID());
                        if (it == clients_.end()) {
                            std::stringstream ss;
                            ss << "ERROR: Received [clientID="<< hdr.clientID() << ",requestID="<< hdr.requestID << ",message=" << hdr.message << ",payload=" << hdr.payloadSize << "]" << std::endl;
                            ss << "Unexpected answer for clientID recieved (" << hdr.clientID() << "). ABORTING";
                            eckit::Log::status() << ss.str() << std::endl;
                            std::cerr << "Retrieving... " << ss.str() << std::endl;
                            throw eckit::SeriousBug(ss.str(), Here());
                        }
                        client = it->second;
                    }

                    ASSERT(client);
                    ASSERT(!hdr.control());
                    if (hdr.payloadSize == 0) {
                        handled = client->handle(hdr.message, hdr.control(), hdr.requestID);
                    }
                    else {
                        handled = client->handle(hdr.message, hdr.control(), hdr.requestID, std::move(payload));
                    }

                    if (!handled) {
                        std::stringstream ss;
                        ss << "ERROR: DATA connection=" << controlEndpoint_ << " Unexpected message recieved (" << hdr.message << "). ABORTING";
                        eckit::Log::status() << ss.str() << std::endl;
                        std::cerr << "Client Retrieving... " << ss.str() << std::endl;
                        throw eckit::SeriousBug(ss.str(), Here());
                    }
                }
            }
        }

    // We don't want to let exceptions escape inside a worker thread.
    } catch (const std::exception& e) {
       ClientConnectionRouter::instance().teardown(std::make_exception_ptr(e));
    } catch (...) {
       ClientConnectionRouter::instance().teardown(std::current_exception());
    }
    // ClientConnectionRouter::instance().deregister(*this);
}

}  // namespace fdb5::remote
