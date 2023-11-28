

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

// using namespace eckit;

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

namespace{
    
class ConnectionError : public eckit::Exception {
public:
    ConnectionError(const int);
    ConnectionError(const int, const eckit::net::Endpoint&);

    bool retryOnClient() const override { return true; } 
};

ConnectionError::ConnectionError(const int retries) {
    std::ostringstream s;
    s << "Unable to create a connection with the FDB server after " << retries << " retries";
    reason(s.str());
    eckit::Log::status() << what() << std::endl;
}

ConnectionError::ConnectionError(const int retries, const eckit::net::Endpoint& endpoint) {
    std::ostringstream s;
    s << "Unable to create a connection with the FDB endpoint " << endpoint << " after " << retries << " retries";
    reason(s.str());
    eckit::Log::status() << what() << std::endl;
}

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


ClientConnection::ClientConnection(const eckit::net::Endpoint& controlEndpoint):
    controlEndpoint_(controlEndpoint), id_(0),
    connected_(false) {
        eckit::Log::debug<LibFdb5>() << "ClientConnection::ClientConnection() controlEndpoint: " << controlEndpoint_ << std::endl;
    }


ClientConnection::~ClientConnection() {
    disconnect();
}

uint32_t ClientConnection::generateRequestID() {

    std::lock_guard<std::mutex> lock(idMutex_);
    return ++id_;
}

void ClientConnection::connect() {

    std::lock_guard<std::mutex> lock(requestMutex_);

    if (connected_) {
        eckit::Log::warning() << "ClientConnection::connect() called when already connected" << std::endl;
        return;
    }

    static int fdbMaxConnectRetries = eckit::Resource<int>("fdbMaxConnectRetries", 5);
    static int fdbConnectTimeout = eckit::Resource<int>("fdbConnectTimeout", 0); // No timeout

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
    } catch(eckit::TooManyRetries& e) {
        if (controlClient_.isConnected()) {
            controlClient_.close();
            throw ConnectionError(fdbMaxConnectRetries, dataEndpoint_);
        } else {
            throw ConnectionError(fdbMaxConnectRetries, controlEndpoint_);
        }
    }
}

void ClientConnection::disconnect() {

    std::lock_guard<std::mutex> lock(requestMutex_);

    if (connected_) {

        // Send termination message
        controlWrite(Message::Exit);

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
const eckit::net::Endpoint& ClientConnection::dataEndpoint() const { 
    return dataEndpoint_;
} 

eckit::LocalConfiguration ClientConnection::availableFunctionality() const {
    eckit::LocalConfiguration conf;
    std::vector<int> remoteFieldLocationVersions = {1};
    conf.set("RemoteFieldLocation", remoteFieldLocationVersions);
    return conf;
}

// -----------------------------------------------------------------------------------------------------

void ClientConnection::addRequest(Client& client, uint32_t requestID) {
    ASSERT(requestID);

    std::lock_guard<std::mutex> lock(requestMutex_);
    requests_[requestID] = &client;
}

void ClientConnection::controlWrite(Client& client, Message msg, uint32_t requestID, std::vector<std::pair<const void*, uint32_t>> data) {

    if (requestID) {
        addRequest(client, requestID);
    }

    controlWrite(msg, requestID, data);
}

void ClientConnection::controlWrite(Message msg, uint32_t requestID, std::vector<std::pair<const void*, uint32_t>> data) {

    uint32_t payloadLength = 0;
    for (auto d: data) {
        ASSERT(d.first);
        payloadLength += d.second;
    }
    eckit::Log::debug<LibFdb5>() << "ClientConnection::controlWrite [endpoint=" << controlEndpoint_.hostport() <<
        ",message=" << ((int) msg) << ",requestID=" << requestID << ",data=" << data.size() << ",payload=" << payloadLength << "]" << std::endl;

    MessageHeader message(msg, requestID, payloadLength);

    std::lock_guard<std::mutex> lock(controlMutex_);
    controlWrite(&message, sizeof(message));
    for (auto d: data) {
        controlWrite(d.first, d.second);
    }
    controlWrite(&EndMarker, sizeof(EndMarker));
}

void ClientConnection::controlWrite(const void* data, size_t length) {
    size_t written = controlClient_.write(data, length);
    if (length != written) {
        std::stringstream ss;
        ss << "Write error. Expected " << length << " bytes, wrote " << written;
        throw TCPException(ss.str(), Here());
    }
}

void ClientConnection::controlRead(void* data, size_t length) {
    //std::cout << "ClientConnection::controlRead " << this << std::endl;
    size_t read = controlClient_.read(data, length);
    if (length != read) {
        std::stringstream ss;
        ss << "Read error. Expected " << length << " bytes, read " << read;
        throw TCPException(ss.str(), Here());
    }
}


void ClientConnection::dataWrite(Client& client, remote::Message msg, uint32_t requestID, std::vector<std::pair<const void*, uint32_t>> data) {

    if (requestID) {
        auto it = requests_.find(requestID);
        if (it != requests_.end()) {
            ASSERT(it->second == &client);
        } else {
            addRequest(client, requestID);
        }
    }

    dataWrite(msg, requestID, data);
}

void ClientConnection::dataWrite(remote::Message msg, uint32_t requestID, std::vector<std::pair<const void*, uint32_t>> data) {

    uint32_t payloadLength = 0;
    for (auto d: data) {
        ASSERT(d.first);
        payloadLength += d.second;
    }
    MessageHeader message(msg, requestID, payloadLength);

    eckit::Log::debug<LibFdb5>() << "ClientConnection::dataWrite [endpoint=" << dataEndpoint_.hostport() <<
        ",message=" << ((int) msg) << ",requestID=" << requestID << ",data=" << data.size() << ",payload=" << payloadLength << "]" << std::endl;

    std::lock_guard<std::mutex> lock(dataMutex_);
    dataWrite(&message, sizeof(message));
    for (auto d: data) {
        dataWrite(d.first, d.second);
    }
    dataWrite(&EndMarker, sizeof(EndMarker));
}

void ClientConnection::dataWrite(const void* data, size_t length) {
    size_t written = dataClient_.write(data, length);
    if (length != written) {
        std::stringstream ss;
        ss << "Write error. Expected " << length << " bytes, wrote " << written;
        throw TCPException(ss.str(), Here());
    }
}

void ClientConnection::dataRead(void* data, size_t length) {
    size_t read = dataClient_.read(data, length);
    if (length != read) {
        std::stringstream ss;
        ss << "Read error. Expected " << length << " bytes, read " << read;
        throw TCPException(ss.str(), Here());
    }
}

void ClientConnection::handleError(const MessageHeader& hdr) {

    ASSERT(hdr.marker == StartMarker);
    ASSERT(hdr.version == CurrentVersion);

    if (hdr.message == Message::Error) {
        ASSERT(hdr.payloadSize > 9);

        std::string what(hdr.payloadSize, ' ');
        dataRead(&what[0], hdr.payloadSize);
        what[hdr.payloadSize] = 0; // Just in case

        try {
            eckit::FixedString<4> tail;
            controlRead(&tail, sizeof(tail));
        } catch (...) {}

        throw RemoteFDBException(what, controlEndpoint_);
    }
}

void ClientConnection::writeControlStartupMessage() {

    eckit::Buffer payload(4096);
    eckit::MemoryStream s(payload);
    s << sessionID_;
    s << controlEndpoint_;
    s << LibFdb5::instance().remoteProtocolVersion().used();

    // TODO: Abstract this dictionary into a RemoteConfiguration object, which
    //       understands how to do the negotiation, etc, but uses Value (i.e.
    //       essentially JSON) over the wire for flexibility.
    s << availableFunctionality().get();

    //std::cout << "writeControlStartupMessage" << std::endl;
    controlWrite(Message::Startup, 0, std::vector<std::pair<const void*, uint32_t>>{{payload, s.position()}});
}

void ClientConnection::writeDataStartupMessage(const eckit::SessionID& serverSession) {

    eckit::Buffer payload(1024);
    eckit::MemoryStream s(payload);

    s << sessionID_;
    s << serverSession;

    dataWrite(Message::Startup, 0, std::vector<std::pair<const void*, uint32_t>>{{payload, s.position()}});
}

eckit::SessionID ClientConnection::verifyServerStartupResponse() {

    MessageHeader hdr;
    controlRead(&hdr, sizeof(hdr));

    ASSERT(hdr.marker == StartMarker);
    ASSERT(hdr.version == CurrentVersion);
    ASSERT(hdr.message == Message::Startup);
    ASSERT(hdr.requestID == 0);

    eckit::Buffer payload(hdr.payloadSize);
    eckit::FixedString<4> tail;
    controlRead(payload, hdr.payloadSize);
    controlRead(&tail, sizeof(tail));
    ASSERT(tail == EndMarker);

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

    return serverSession;
}

void ClientConnection::listeningThreadLoop() {

    try {

        MessageHeader hdr;
        eckit::FixedString<4> tail;

        while (true) {

            dataRead(&hdr, sizeof(hdr));

            eckit::Log::debug<LibFdb5>() << "ClientConnection::listeningThreadLoop - got [message=" << ((int) hdr.message) << ",requestID=" << hdr.requestID << ",payload=" << hdr.payloadSize << "]" << std::endl;

            ASSERT(hdr.marker == StartMarker);
            ASSERT(hdr.version == CurrentVersion);

            if (hdr.message == Message::Exit) {
                return;
            }

            if (hdr.requestID) {
                bool handled = false;
                auto it = requests_.find(hdr.requestID);
                if (it == requests_.end()) {
                    std::stringstream ss;
                    ss << "ERROR: Unexpected answer to requestID recieved (" << hdr.requestID << "). ABORTING";
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
                        handled = it->second->handle(hdr.message, hdr.requestID);
                    }
                }
                else {
                    eckit::Buffer payload{hdr.payloadSize};
                    dataRead(payload, hdr.payloadSize);

                    if (it->second->blockingRequestId() == hdr.requestID) {
                        handled = it->second->response(hdr.requestID, std::move(payload));
                    } else {
                        handled = it->second->handle(hdr.message, hdr.requestID, std::move(payload));
                    }
                }

                if (!handled) {
                    std::stringstream ss;
                    ss << "ERROR: Unexpected message recieved (" << static_cast<int>(hdr.message) << "). ABORTING";
                    eckit::Log::status() << ss.str() << std::endl;
                    eckit::Log::error() << "Client Retrieving... " << ss.str() << std::endl;
                    throw eckit::SeriousBug(ss.str(), Here());
                }
            } else {
                if (hdr.payloadSize) {
                    eckit::Buffer payload{hdr.payloadSize};
                    dataRead(payload, hdr.payloadSize);
                }
            }

            // Ensure we have consumed exactly the correct amount from the socket.
            dataRead(&tail, sizeof(tail));
            ASSERT(tail == EndMarker);
        }

    // We don't want to let exceptions escape inside a worker thread.
    } catch (const std::exception& e) {
//        ClientConnectionRouter::instance().handleException(std::make_exception_ptr(e));
    } catch (...) {
//        ClientConnectionRouter::instance().handleException(std::current_exception());
    }
}

}  // namespace fdb5::remote
