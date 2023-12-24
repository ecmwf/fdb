/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#include <chrono>

#include "eckit/config/Resource.h"
#include "eckit/maths/Functions.h"
#include "eckit/net/Endpoint.h"
#include "eckit/runtime/Main.h"
#include "eckit/runtime/SessionID.h"
#include "eckit/serialisation/MemoryStream.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/fdb5_version.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/database/Key.h"
#include "fdb5/remote/AvailablePortList.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/remote/RemoteFieldLocation.h"
#include "fdb5/api/FDB.h"

#include "fdb5/remote/server/ServerConnection.h"

namespace fdb5::remote {

// helpers
namespace {

class TCPException : public eckit::Exception {
public:
    TCPException(const std::string& msg, const eckit::CodeLocation& here) :
        eckit::Exception(std::string("TCPException: ") + msg, here) {}
};

std::vector<int> intersection(const eckit::LocalConfiguration& c1, const eckit::LocalConfiguration& c2, const std::string& field){

    std::vector<int> v1 = c1.getIntVector(field);
    std::vector<int> v2 = c2.getIntVector(field);
    std::vector<int> v3;

    std::sort(v1.begin(), v1.end());
    std::sort(v2.begin(), v2.end());

    std::set_intersection(v1.begin(),v1.end(),
                          v2.begin(),v2.end(),
                          back_inserter(v3));
    return v3;
}

} // namespace

ServerConnection::ServerConnection(eckit::net::TCPSocket& socket, const Config& config) :
        config_(config),
        controlSocket_(socket),
        dataSocket_(selectDataPort()),
        dataListenHostname_(config.getString("dataListenHostname", "")),
        readLocationQueue_(eckit::Resource<size_t>("fdbRetrieveQueueSize", 10000)) {
            eckit::Log::debug<LibFdb5>() << "ServerConnection::ServerConnection initialized" << std::endl;
    }

ServerConnection::~ServerConnection() {
    // We don't want to die before the worker threads are cleaned up
    waitForWorkers();

    // And notify the client that we are done.
    eckit::Log::info() << "Sending exit message to client" << std::endl;
    dataWrite(Message::Exit, 0, 0);
    eckit::Log::info() << "Done" << std::endl;
}

eckit::LocalConfiguration ServerConnection::availableFunctionality() const {
    eckit::LocalConfiguration conf;
//    Add to the configuration all the components that require to be versioned, as in the following example, with a vector of supported version numbers
    std::vector<int> remoteFieldLocationVersions = {1};
    conf.set("RemoteFieldLocation", remoteFieldLocationVersions);
    return conf;
}

void ServerConnection::initialiseConnections() {
    // Read the startup message from the client. Check that it all checks out.

    MessageHeader hdr;
    socketRead(&hdr, sizeof(hdr), controlSocket_);

    ASSERT(hdr.marker == StartMarker);
    ASSERT(hdr.version == CurrentVersion);
    ASSERT(hdr.message == Message::Startup);
    ASSERT(hdr.requestID == 0);

    eckit::Buffer payload1 = receivePayload(hdr, controlSocket_);
    eckit::FixedString<4> tail;
    socketRead(&tail, sizeof(tail), controlSocket_);
    ASSERT(tail == EndMarker);

    eckit::MemoryStream s1(payload1);
    eckit::SessionID clientSession(s1);
    eckit::net::Endpoint endpointFromClient(s1);
    unsigned int remoteProtocolVersion = 0;
    std::string errorMsg;

    try {
        s1 >> remoteProtocolVersion;
    } catch (...) {
        errorMsg = "Error retrieving client protocol version";
    }

    if (errorMsg.empty() && !LibFdb5::instance().remoteProtocolVersion().check(remoteProtocolVersion, false)) {
        std::stringstream ss;
        ss << "FDB server version " << fdb5_version_str() << " - remote protocol version not supported:" << std::endl;
        ss << "    versions supported by server: " << LibFdb5::instance().remoteProtocolVersion().supportedStr() << std::endl;
        ss << "    version requested by client: " << remoteProtocolVersion << std::endl;
        errorMsg = ss.str();
    }

    if (errorMsg.empty()) {
        eckit::LocalConfiguration clientAvailableFunctionality(s1);
        eckit::LocalConfiguration serverConf = availableFunctionality();
        agreedConf_ = eckit::LocalConfiguration();

        // agree on a common functionality by intersecting server and client version numbers
        std::vector<int> rflCommon = intersection(clientAvailableFunctionality, serverConf, "RemoteFieldLocation");
        if (rflCommon.size() > 0) {
            eckit::Log::debug<LibFdb5>() << "Protocol negotiation - RemoteFieldLocation version " << rflCommon.back() << std::endl;
            agreedConf_.set("RemoteFieldLocation", rflCommon.back());
        }
        else {
            std::stringstream ss;
            ss << "FDB server version " << fdb5_version_str() << " - failed protocol negotiation with FDB client" << std::endl;
            ss << "    server functionality: " << serverConf << std::endl;
            ss << "    client functionality: " << clientAvailableFunctionality << std::endl;
            errorMsg = ss.str();
        }
    }

    // We want a data connection too. Send info to RemoteFDB, and wait for connection
    // n.b. FDB-192: we use the host communicated from the client endpoint. This
    //               ensures that if a specific interface has been selected and the
    //               server has multiple, then we use that on, whilst retaining
    //               the capacity in the protocol for the server to make a choice.

    int dataport = dataSocket_.localPort();
    eckit::net::Endpoint dataEndpoint(endpointFromClient.hostname(), dataport);

    eckit::Log::info() << "Sending data endpoint to client: " << dataEndpoint << std::endl;
    {
        eckit::Buffer startupBuffer(1024);
        eckit::MemoryStream s(startupBuffer);

        s << clientSession;
        s << sessionID_;
        s << dataEndpoint;
        s << agreedConf_.get();
        // s << storeEndpoint; // xxx single-store case only: we cant do this with multiple stores // For now, dont send the store endpoint to the client 

        eckit::Log::debug<LibFdb5>() << "Protocol negotiation - configuration: " << agreedConf_ <<std::endl;

        controlWrite(Message::Startup, 0, 0, startupBuffer.data(), s.position());
    }


    if (!errorMsg.empty()) {
        controlWrite(Message::Error, 0, 0, errorMsg.c_str(), errorMsg.length());
        return;
    }

    dataSocket_.accept();

    // Check the response from the client.
    // Ensure that the hostname matches the original hostname, and that
    // it returns the details we sent it
    // IE check that we are connected to the correct client!

    MessageHeader dataHdr;
    socketRead(&dataHdr, sizeof(dataHdr), dataSocket_);

    ASSERT(dataHdr.marker == StartMarker);
    ASSERT(dataHdr.version == CurrentVersion);
    ASSERT(dataHdr.message == Message::Startup);
    ASSERT(dataHdr.requestID == 0);

    eckit::Buffer payload2 = receivePayload(dataHdr, dataSocket_);
    socketRead(&tail, sizeof(tail), dataSocket_);
    ASSERT(tail == EndMarker);

    eckit::MemoryStream s2(payload2);
    eckit::SessionID clientSession2(s2);
    eckit::SessionID serverSession(s2);

    if (clientSession != clientSession2) {
        std::stringstream ss;
        ss << "Client session IDs do not match: " << clientSession << " != " << clientSession2;
        throw eckit::BadValue(ss.str(), Here());
    }

    if (serverSession != sessionID_) {
        std::stringstream ss;
        ss << "Session IDs do not match: " << serverSession << " != " << sessionID_;
        throw eckit::BadValue(ss.str(), Here());
    }
}

// Handler& ServerConnection::handler(uint32_t id) {
//     auto it = handlers_.find(id);
//     ASSERT(it != handlers_.end());

//     return *it->second;
// }

// Handler& ServerConnection::handler(uint32_t id, eckit::Buffer payload) {

//     ASSERT(handlers_.find(id) == handlers_.end());



//     auto it = handlers_.find(id);
//     ASSERT(it != handlers_.end());

//     return *it->second;
// }

int ServerConnection::selectDataPort() {
    eckit::Log::info() << "SelectDataPort: " << std::endl;
    eckit::Log::info() << config_ << std::endl;
    if (config_.has("dataPortStart")) {
        ASSERT(config_.has("dataPortCount"));
        return AvailablePortList(config_.getInt("dataPortStart"), config_.getLong("dataPortCount"))
            .acquire();
    }

    // Use a system assigned port
    return 0;
}

void ServerConnection::controlWrite(Message msg, uint32_t clientID, uint32_t requestID, const void* payload, uint32_t payloadLength) {
    ASSERT((payload == nullptr) == (payloadLength == 0));

    MessageHeader message(msg, clientID, requestID, payloadLength);
    std::lock_guard<std::mutex> lock(controlWriteMutex_);
    controlWrite(&message, sizeof(message));
    if (payload) {
        controlWrite(payload, payloadLength);
    }
    controlWrite(&EndMarker, sizeof(EndMarker));
}

void ServerConnection::controlWrite(const void* data, size_t length) {
    size_t written = controlSocket_.write(data, length);
    if (length != written) {
        std::stringstream ss;
        ss << "Write error. Expected " << length << " bytes, wrote " << written;
        throw TCPException(ss.str(), Here());
    }
}

void ServerConnection::socketRead(void* data, size_t length, eckit::net::TCPSocket& socket) {

    size_t read = socket.read(data, length);
    if (length != read) {
        std::stringstream ss;
        ss << "Read error. Expected " << length << " bytes, read " << read;
        throw TCPException(ss.str(), Here());
    }
}

void ServerConnection::archive(const MessageHeader& hdr) {

    ASSERT(hdr.payloadSize == 0);

    auto archive = archiveFuture_.find(hdr.clientID);

    // Ensure that we aren't already running a catalogue/store
    if(archive == archiveFuture_.end() || !archive->second.valid()) {
        // Start archive worker thread
        uint32_t archiverID = hdr.clientID;
        archiveFuture_[hdr.clientID] = std::async(std::launch::async, [this, archiverID] { return archiveThreadLoop(archiverID); });
    }
}

void ServerConnection::dataWrite(Message msg, uint32_t clientID, uint32_t requestID, const void* payload, uint32_t payloadLength) {

    eckit::Log::debug<LibFdb5>() << "ServerConnection::dataWrite [message="<< static_cast<int>(msg) << ",requestID=" << requestID << ",payloadLength=" << payloadLength << "]" << std::endl;

    ASSERT((payload == nullptr) == (payloadLength == 0));

    MessageHeader message(msg, clientID, requestID, payloadLength);

    std::lock_guard<std::mutex> lock(dataWriteMutex_);
    dataWriteUnsafe(&message, sizeof(message));
    if (payload) {
        dataWriteUnsafe(payload, payloadLength);
    }
    dataWriteUnsafe(&EndMarker, sizeof(EndMarker));
}

void ServerConnection::dataWriteUnsafe(const void* data, size_t length) {
    size_t written = dataSocket_.write(data, length);
    if (length != written) {
        std::stringstream ss;
        ss << "Write error. Expected " << length << " bytes, wrote " << written;
        throw TCPException(ss.str(), Here());
    }
}

eckit::Buffer ServerConnection::receivePayload(const MessageHeader& hdr, eckit::net::TCPSocket& socket) {
    eckit::Buffer payload(hdr.payloadSize);

    ASSERT(hdr.payloadSize > 0);
    socketRead(payload, hdr.payloadSize, socket);

    return payload;
}

void ServerConnection::tidyWorkers() {
    std::map<uint32_t, std::future<void>>::iterator it = workerThreads_.begin();

    for (; it != workerThreads_.end(); /* empty */) {
        std::future_status stat = it->second.wait_for(std::chrono::milliseconds(0));

        if (stat == std::future_status::ready) {
            eckit::Log::info() << "Tidying up worker for request ID: " << it->first << std::endl;
            workerThreads_.erase(it++);
        }
        else {
            ++it;
        }
    }
}

void ServerConnection::waitForWorkers() {
    readLocationQueue_.close();

    tidyWorkers();

    for (auto& it : workerThreads_) {
        eckit::Log::error() << "Worker thread still alive for request ID: " << it.first << std::endl;
        eckit::Log::error() << "Joining ..." << std::endl;
        it.second.get();
        eckit::Log::error() << "Thread complete" << std::endl;
    }

    if (readLocationWorker_.joinable()) {
        readLocationWorker_.join();
    }
}

}  // namespace fdb5::remote
