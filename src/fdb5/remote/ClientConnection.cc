

#include <functional>
#include <unistd.h>

#include "fdb5/remote/ClientConnection.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/remote/RemoteFieldLocation.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/database/Key.h"

#include "eckit/config/LocalConfiguration.h"
#include "eckit/io/Buffer.h"
#include "eckit/log/Bytes.h"
#include "eckit/log/Log.h"
#include "eckit/message/Message.h"
#include "eckit/config/Resource.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/utils/Translator.h"
#include "eckit/runtime/Main.h"
#include "eckit/os/BackTrace.h"

#include "metkit/mars/MarsRequest.h"

using namespace eckit;
using namespace eckit::net;
// using namespace fdb5::remote;

namespace fdb5 {
namespace remote {

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
    Log::status() << what() << std::endl;
}

ConnectionError::ConnectionError(const int retries, const eckit::net::Endpoint& endpoint) {
    std::ostringstream s;
    s << "Unable to create a connection with the FDB endpoint " << endpoint << " after " << retries << " retries";
    reason(s.str());
    Log::status() << what() << std::endl;
}

class TCPException : public Exception {
public:
    TCPException(const std::string& msg, const CodeLocation& here) :
        Exception(std::string("TCPException: ") + msg, here) {

        eckit::Log::error() << "TCP Exception; backtrace(): " << std::endl;
        eckit::Log::error() << eckit::BackTrace::dump() << std::endl;
    }
};

}

//----------------------------------------------------------------------------------------------------------------------


ClientConnection::ClientConnection(const eckit::net::Endpoint& controlEndpoint, const eckit::Configuration& config):
    controlEndpoint_(controlEndpoint),
    connected_(false) {
        Log::debug<LibFdb5>() << "ClientConnection::ClientConnection() controlEndpoint: " << controlEndpoint_ << std::endl;
    }


ClientConnection::~ClientConnection() {
    disconnect();
}

void ClientConnection::connect() {

    if (connected_) {
        Log::warning() << "ClientConnection::connect() called when already connected" << std::endl;
        return;
    }

    static int fdbMaxConnectRetries = eckit::Resource<int>("fdbMaxConnectRetries", 5);

    try {
        // Connect to server, and check that the server is happy on the response

        Log::debug<LibFdb5>() << "Connecting to host: " << controlEndpoint_ << std::endl;
        controlClient_.connect(controlEndpoint_, fdbMaxConnectRetries);
        writeControlStartupMessage();
        SessionID serverSession = verifyServerStartupResponse();

        // Connect to the specified data port
        Log::debug<LibFdb5>() << "Received data endpoint from host: " << dataEndpoint_ << std::endl;
        dataClient_.connect(dataEndpoint_, fdbMaxConnectRetries);
        writeDataStartupMessage(serverSession);

        // And the connections are set up. Let everything start up!
        listeningThread_ = std::thread([this] { listeningThreadLoop(); });
        connected_ = true;
    } catch(TooManyRetries& e) {
        if (controlClient_.isConnected()) {
            controlClient_.close();
            throw ConnectionError(fdbMaxConnectRetries, dataEndpoint_);
        } else {
            throw ConnectionError(fdbMaxConnectRetries, controlEndpoint_);
        }
    }
}
SessionID ClientConnection::verifyServerStartupResponse() {

    MessageHeader hdr;
    controlRead(&hdr, sizeof(hdr));

    ASSERT(hdr.marker == StartMarker);
    ASSERT(hdr.version == CurrentVersion);
    ASSERT(hdr.message == Message::Startup);
    ASSERT(hdr.requestID == 0);

    Buffer payload(hdr.payloadSize);
    eckit::FixedString<4> tail;
    controlRead(payload, hdr.payloadSize);
    controlRead(&tail, sizeof(tail));
    ASSERT(tail == EndMarker);

    MemoryStream s(payload);
    SessionID clientSession(s);
    SessionID serverSession(s);
    Endpoint dataEndpoint(s);
    LocalConfiguration serverFunctionality(s);

    dataEndpoint_ = dataEndpoint;

    if (dataEndpoint_.hostname() != controlEndpoint_.hostname()) {
        Log::warning() << "Data and control interface hostnames do not match. "
                       << dataEndpoint_.hostname() << " /= "
                       << controlEndpoint_.hostname() << std::endl;
    }

    if (clientSession != sessionID_) {
        std::stringstream ss;
        ss << "Session ID does not match session received from server: "
           << sessionID_ << " != " << clientSession;
        throw BadValue(ss.str(), Here());
    }

    return serverSession;
}

void ClientConnection::writeControlStartupMessage() {

    Buffer payload(4096);
    MemoryStream s(payload);
    s << sessionID_;
    s << controlEndpoint_;
    s << LibFdb5::instance().remoteProtocolVersion().used();

    // TODO: Abstract this dictionary into a RemoteConfiguration object, which
    //       understands how to do the negotiation, etc, but uses Value (i.e.
    //       essentially JSON) over the wire for flexibility.
    s << availableFunctionality().get();

    controlWrite(Message::Startup, 0, payload.data(), s.position());
}

eckit::LocalConfiguration ClientConnection::availableFunctionality() const {
    eckit::LocalConfiguration conf;
    std::vector<int> remoteFieldLocationVersions = {1};
    conf.set("RemoteFieldLocation", remoteFieldLocationVersions);
    return conf;
}

void ClientConnection::disconnect() {
    if (connected_) {

        // Send termination message
        controlWrite(Message::Exit, generateRequestID()); //xxx why do we generate a request ID here? 

        listeningThread_.join();

        // Close both the control and data connections
        controlClient_.close();
        dataClient_.close();
        connected_ = false;
    }
}

void ClientConnection::writeDataStartupMessage(const eckit::SessionID& serverSession) {

    Buffer payload(1024);
    MemoryStream s(payload);

    s << sessionID_;
    s << serverSession;

    dataWrite(Message::Startup, 0, payload.data(), s.position());
}

// -----------------------------------------------------------------------------------------------------

void ClientConnection::controlWriteCheckResponse(Message msg, uint32_t requestID, const void* payload, uint32_t payloadLength) {

    controlWrite(msg, requestID, payload, payloadLength);

    // Wait for the receipt acknowledgement

    MessageHeader response;
    controlRead(&response, sizeof(MessageHeader));

    handleError(response);

    ASSERT(response.marker == StartMarker);
    ASSERT(response.version == CurrentVersion);
    ASSERT(response.message == Message::Received);

    eckit::FixedString<4> tail;
    controlRead(&tail, sizeof(tail));
    ASSERT(tail == EndMarker);
}

void ClientConnection::controlWrite(Message msg, uint32_t requestID, const void* payload, uint32_t payloadLength) {

    ASSERT((payload == nullptr) == (payloadLength == 0));

    MessageHeader message(msg, requestID, payloadLength);
    controlWrite(&message, sizeof(message));
    if (payload) {
        controlWrite(payload, payloadLength);
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
    size_t read = controlClient_.read(data, length);
    if (length != read) {
        std::stringstream ss;
        ss << "Read error. Expected " << length << " bytes, read " << read;
        throw TCPException(ss.str(), Here());
    }
}

void ClientConnection::dataWrite(Message msg, uint32_t requestID, const void* payload, uint32_t payloadLength) {

    ASSERT((payload == nullptr) == (payloadLength == 0));
    MessageHeader message(msg, requestID, payloadLength);
    dataWrite(&message, sizeof(message));
    if (payload) {
        dataWrite(payload, payloadLength);
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
        controlRead(&what[0], hdr.payloadSize);
        what[hdr.payloadSize] = 0; // Just in case

        try {
            eckit::FixedString<4> tail;
            controlRead(&tail, sizeof(tail));
        } catch (...) {}

        throw DecoupledFDBException(what, controlEndpoint_);
    }
}

void ClientConnection::index(const Key& key, const FieldLocation& location) {
    NOTIMP;
}

// void ClientConnection::store(const Key& key, const void* data, size_t length) {
      
//     // if there is no archiving thread active, then start one.
//     // n.b. reset the archiveQueue_ after a potential flush() cycle.

//     if (!archiveFuture_.valid()) {

//         // Start the archival request on the remote side
//         ASSERT(archiveID_ == 0);
//         uint32_t id = generateRequestID();
//         controlWriteCheckResponse(Message::Archive, id);
//         archiveID_ = id;

//         // Reset the queue after previous done/errors
//         {
//             std::lock_guard<std::mutex> lock(archiveQueuePtrMutex_);
//             ASSERT(!archiveQueue_);
//             archiveQueue_.reset(new ArchiveQueue(maxArchiveQueueLength_));
//         }

//         archiveFuture_ = std::async(std::launch::async, [this, id] { return archiveThreadLoop(id); });
//     }

//     ASSERT(archiveFuture_.valid());
//     ASSERT(archiveID_ != 0);
//     {
//         std::lock_guard<std::mutex> lock(archiveQueuePtrMutex_);
//         ASSERT(archiveQueue_);
//         archiveQueue_->emplace(std::make_pair(key, Buffer(reinterpret_cast<const char*>(data), length)));
//     }
// }


}  // namespace remote
}  // namespace fdb5