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

#include "fdb5/remote/server/ServerConnection.h"

#include "eckit/config/Resource.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/fdb5_version.h"
#include "fdb5/remote/Connection.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/remote/server/AvailablePortList.h"

#include "eckit/config/LocalConfiguration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/log/CodeLocation.h"
#include "eckit/log/Log.h"
#include "eckit/net/Endpoint.h"
#include "eckit/net/TCPServer.h"
#include "eckit/net/TCPSocket.h"
#include "eckit/runtime/SessionID.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/utils/Literals.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <future>
#include <iterator>
#include <map>
#include <mutex>
#include <ostream>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace eckit::literals;

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------
// helpers

namespace {

constexpr const auto defaultRetrieveQueueSize = 10000;
constexpr const auto defaultArchiveQueueSize  = 320;

std::vector<int> intersection(const eckit::LocalConfiguration& c1, const eckit::LocalConfiguration& c2,
                              const std::string& field) {

    std::vector<int> v1 = c1.getIntVector(field);
    std::vector<int> v2 = c2.getIntVector(field);
    std::vector<int> v3;

    std::sort(v1.begin(), v1.end());
    std::sort(v2.begin(), v2.end());

    std::set_intersection(v1.begin(), v1.end(), v2.begin(), v2.end(), back_inserter(v3));
    return v3;
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

ServerConnection::ServerConnection(eckit::net::TCPSocket& socket, const Config& config) :
    config_(config),
    dataListenHostname_(config.getString("dataListenHostname", "")),
    readLocationQueue_(eckit::Resource<size_t>("fdbRetrieveQueueSize", defaultRetrieveQueueSize)),
    archiveQueue_(eckit::Resource<size_t>("fdbServerMaxQueueSize", defaultArchiveQueueSize)),
    controlSocket_(socket) {

    LOG_DEBUG_LIB(LibFdb5) << "ServerConnection::ServerConnection initialized" << std::endl;
}

ServerConnection::~ServerConnection() {
    // We don't want to die before the worker threads are cleaned up
    waitForWorkers();

    if (archiveFuture_.valid()) {
        archiveFuture_.wait();
    }

    eckit::Log::info() << "Done" << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

Handled ServerConnection::handleData(Message message, uint32_t clientID, uint32_t requestID) {
    try {
        switch (message) {
            case Message::Flush:  // notification that the client has sent all data for archival
                return Handled::YesRemoveArchiveListener;  // ????

            default: {
                std::stringstream ss;
                ss << "ERROR: Unexpected message recieved (" << message << "). ABORTING";
                eckit::Log::status() << ss.str() << std::endl;
                eckit::Log::error() << ss.str() << std::endl;
                throw eckit::SeriousBug(ss.str(), Here());
            }
        }
    }
    catch (std::exception& e) {
        // n.b. more general than eckit::Exception
        error(e.what(), clientID, requestID);
    }
    catch (...) {
        error("Caught unexpected and unknown error", clientID, requestID);
    }
    return Handled::No;
}

Handled ServerConnection::handleData(Message message, uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {
    try {
        switch (message) {
            case Message::Blob:
            case Message::MultiBlob:
                queue(message, clientID, requestID, std::move(payload));
                return Handled::Replied;

            default: {
                std::stringstream ss;
                ss << "ERROR: Unexpected message recieved (" << message << "). ABORTING";
                eckit::Log::status() << ss.str() << std::endl;
                eckit::Log::error() << ss.str() << std::endl;
                throw eckit::SeriousBug(ss.str(), Here());
            }
        }
    }
    catch (std::exception& e) {
        // n.b. more general than eckit::Exception
        error(e.what(), clientID, requestID);
    }
    catch (...) {
        error("Caught unexpected and unknown error", clientID, requestID);
    }
    return Handled::No;
}

eckit::LocalConfiguration ServerConnection::availableFunctionality() const {
    eckit::LocalConfiguration conf;
    //    Add to the configuration all the components that require to be versioned, as in the following example, with a
    //    vector of supported version numbers
    static std::vector<int> remoteFieldLocationVersions = {1};
    static std::vector<int> numberOfConnections         = config_.getIntVector("supportedConnections", {1, 2});

    ASSERT(0 < numberOfConnections.size());
    ASSERT(numberOfConnections[0] == 1 || numberOfConnections[0] == 2);

    ASSERT(numberOfConnections.size() <= 2);
    if (numberOfConnections.size() > 1) {
        ASSERT(numberOfConnections[0] == 1);
        ASSERT(numberOfConnections[1] == 2);
    }

    conf.set("RemoteFieldLocation", remoteFieldLocationVersions);
    conf.set("NumberOfConnections", numberOfConnections);
    return conf;
}

void ServerConnection::initialiseConnections() {
    // Read the startup message from the client. Check that it all checks out.

    MessageHeader hdr;
    eckit::Buffer payload1 = readControl(hdr);

    ASSERT(hdr.message == Message::Startup);
    ASSERT(hdr.requestID == 0);

    eckit::MemoryStream s1(payload1);
    eckit::SessionID clientSession(s1);
    eckit::net::Endpoint endpointFromClient(s1);
    unsigned int remoteProtocolVersion = 0;

    try {
        s1 >> remoteProtocolVersion;
    }
    catch (...) {
        error("Error retrieving client protocol version", hdr.clientID(), hdr.requestID);
        return;
    }

    if (!LibFdb5::instance().remoteProtocolVersion().check(remoteProtocolVersion, false)) {
        std::stringstream ss;
        ss << "FDB server version " << fdb5_version_str() << " - remote protocol version not supported:" << std::endl;
        ss << "    versions supported by server: " << LibFdb5::instance().remoteProtocolVersion().supportedStr()
           << std::endl;
        ss << "    version requested by client: " << remoteProtocolVersion << std::endl;
        error(ss.str(), hdr.clientID(), hdr.requestID);
        return;
    }

    eckit::LocalConfiguration clientAvailableFunctionality(s1);
    eckit::LocalConfiguration serverConf = availableFunctionality();
    agreedConf_                          = eckit::LocalConfiguration();
    std::vector<int> rflCommon = intersection(clientAvailableFunctionality, serverConf, "RemoteFieldLocation");

    if (rflCommon.size() > 0) {
        LOG_DEBUG_LIB(LibFdb5) << "Protocol negotiation - RemoteFieldLocation version " << rflCommon.back()
                               << std::endl;
        agreedConf_.set("RemoteFieldLocation", rflCommon.back());
    }
    else {
        std::stringstream ss;
        ss << "FDB server version " << fdb5_version_str()
           << " - RemoteFieldLocation version not matching - impossible to establish a connection" << std::endl;
        error(ss.str(), hdr.clientID(), hdr.requestID);
        return;
    }

    if (!clientAvailableFunctionality.has("NumberOfConnections")) {  // set the default
        std::vector<int> conn = {2};
        clientAvailableFunctionality.set("NumberOfConnections", conn);
    }
    // agree on a common functionality by intersecting server and client version numbers
    std::vector<int> ncCommon = intersection(clientAvailableFunctionality, serverConf, "NumberOfConnections");
    if (ncCommon.size() > 0) {
        int ncSelected = 2;

        if (ncCommon.size() == 1) {
            ncSelected = ncCommon.at(0);
        }
        else {
            ncSelected = ncCommon.back();
            if (clientAvailableFunctionality.has("PreferSingleConnection")) {
                if (std::find(ncCommon.begin(), ncCommon.end(),
                              (clientAvailableFunctionality.getBool("PreferSingleConnection") ? 1 : 2)) !=
                    ncCommon.end()) {
                    ncSelected = (clientAvailableFunctionality.getBool("PreferSingleConnection") ? 1 : 2);
                }
            }
        }

        LOG_DEBUG_LIB(LibFdb5) << "Protocol negotiation - NumberOfConnections " << ncSelected << std::endl;
        agreedConf_.set("NumberOfConnections", ncSelected);
        single_ = (ncSelected == 1);
    }
    else {
        std::stringstream ss;
        ss << "FDB server version " << fdb5_version_str() << " - failed protocol negotiation with FDB client"
           << std::endl;
        ss << "    server functionality: " << serverConf << std::endl;
        ss << "    client functionality: " << clientAvailableFunctionality << std::endl;
        error(ss.str(), hdr.clientID(), hdr.requestID);
        return;
    }

    // We want a data connection too. Send info to RemoteFDB, and wait for connection
    // n.b. FDB-192: we use the host communicated from the client endpoint. This
    //               ensures that if a specific interface has been selected and the
    //               server has multiple, then we use that on, whilst retaining
    //               the capacity in the protocol for the server to make a choice.

    std::future<bool> dataSocketFuture;
    eckit::net::Endpoint dataEndpoint;
    if (single_) {
        dataEndpoint = endpointFromClient;
    }
    else {
        std::lock_guard<std::mutex> lock(dataPortMutex_);
        dataSocket_.reset(new eckit::net::EphemeralTCPServer(selectDataPort()));
        ASSERT(dataSocket_->socket() != -1);

        dataEndpoint = eckit::net::Endpoint{endpointFromClient.hostname(), dataSocket_->localPort()};

        dataSocketFuture = std::async(std::launch::async, [this] {
            dataSocket_->accept();
            return true;
        });
    }

    eckit::Buffer startupBuffer(1_KiB);
    eckit::MemoryStream s(startupBuffer);

    s << clientSession;
    s << sessionID_;
    s << dataEndpoint;
    s << agreedConf_.get();

    LOG_DEBUG_LIB(LibFdb5) << "Protocol negotiation - configuration: " << agreedConf_ << std::endl;

    write(Message::Startup, true, 0, 0, startupBuffer.data(), s.position());

    if (!single_) {
        ASSERT(dataSocketFuture.valid());
        dataSocketFuture.wait();

        // Check the response from the client.
        // Ensure that the hostname matches the original hostname, and that
        // it returns the details we sent it
        // IE check that we are connected to the correct client!

        MessageHeader dataHdr;
        eckit::Buffer payload2 = readData(dataHdr);

        ASSERT(dataHdr.version == MessageHeader::currentVersion);
        ASSERT(dataHdr.message == Message::Startup);
        ASSERT(dataHdr.requestID == 0);

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
}

int ServerConnection::selectDataPort() {
    eckit::Log::info() << "SelectDataPort: " << std::endl;
    eckit::Log::info() << config_ << std::endl;
    if (config_.has("dataPortStart")) {
        ASSERT(config_.has("dataPortCount"));
        return AvailablePortList(config_.getInt("dataPortStart"), config_.getLong("dataPortCount")).acquire();
    }

    // Use a system assigned port
    return 0;
}

size_t ServerConnection::archiveThreadLoop() {
    size_t totalArchived = 0;

    ArchiveElem elem;

    try {
        long queuelen;
        while ((queuelen = archiveQueue_.pop(elem)) != -1) {
            if (elem.multiblob_) {
                // Handle MultiBlob

                const char* firstData = reinterpret_cast<const char*>(elem.payload_.data());  // For pointer arithmetic
                const char* charData  = firstData;
                while (size_t(charData - firstData) < elem.payload_.size()) {
                    const MessageHeader* hdr = reinterpret_cast<const MessageHeader*>(charData);
                    ASSERT(hdr->message == Message::Blob);
                    ASSERT(hdr->clientID() == elem.clientID_);
                    ASSERT(hdr->requestID == elem.requestID_);
                    charData += sizeof(MessageHeader);

                    const void* payloadData = charData;
                    charData += hdr->payloadSize;

                    const auto* e = reinterpret_cast<const MessageHeader::MarkerType*>(charData);
                    ASSERT(*e == MessageHeader::EndMarker);
                    charData += MessageHeader::markerBytes;

                    archiveBlob(elem.clientID_, elem.requestID_, payloadData, hdr->payloadSize);
                    totalArchived += 1;
                }
            }
            else {
                // Handle single blob
                archiveBlob(elem.clientID_, elem.requestID_, elem.payload_.data(), elem.payload_.size());
                totalArchived += 1;
            }
        }
    }
    catch (...) {
        // Ensure exception propagates across the queue back to the parent thread.
        archiveQueue_.interrupt(std::current_exception());
        throw;
    }

    return totalArchived;
}

void ServerConnection::listeningThreadLoopData() {

    MessageHeader hdr;

    try {

        // The archive loop is the only thing that can listen on the data socket,
        // so we don't need to to anything clever here.
        // n.b. we also don't need to lock on read. We are the only thing that reads.
        while (true) {
            eckit::Buffer payload = readData(hdr);  // READ DATA

            if (hdr.message == Message::Exit) {
                ASSERT(hdr.clientID() == 0);

                eckit::Log::status() << "Terminating DATA listener" << std::endl;
                eckit::Log::info() << "Terminating DATA listener" << std::endl;

                break;
            }
            else {

                Handled handled;
                if (payload.size() == 0) {
                    handled = handleData(hdr.message, hdr.clientID(), hdr.requestID);
                }
                else {
                    handled = handleData(hdr.message, hdr.clientID(), hdr.requestID, std::move(payload));
                }

                switch (handled) {
                    case Handled::YesRemoveArchiveListener:
                    case Handled::YesRemoveReadListener: {
                        std::lock_guard<std::mutex> lock(handlerMutex_);
                        numDataListener_--;
                        if (numDataListener_ == 0) {
                            return;
                        }
                        break;
                    }
                    case Handled::Replied:  // nothing to do
                    case Handled::Yes:
                        break;
                    case Handled::No:
                    default:
                        std::stringstream ss;
                        ss << "Unable to handle message " << hdr.message << " received on the data connection";
                        error(ss.str(), hdr.clientID(), hdr.requestID);
                }
            }
        }
    }
    catch (std::exception& e) {
        // n.b. more general than eckit::Exception
        error(e.what(), hdr.clientID(), hdr.requestID);
        throw;
    }
    catch (...) {
        error("Caught unexpected, unknown exception in retrieve worker", hdr.clientID(), hdr.requestID);
        throw;
    }
}

void ServerConnection::handle() {
    initialiseConnections();

    std::thread listeningThreadData;

    MessageHeader hdr;

    // listen loop
    while (true) {
        eckit::Buffer payload = readControl(hdr);  // READ CONTROL
        LOG_DEBUG_LIB(LibFdb5) << "ServerConnection::handle - got [message=" << hdr.message
                               << ",clientID=" << hdr.clientID() << ",requestID=" << hdr.requestID
                               << ",payload=" << hdr.payloadSize << "]" << std::endl;

        if (hdr.message == Message::Stop) {
            ASSERT(hdr.clientID());
            remove(true, hdr.clientID());
        }
        else {
            if (hdr.message == Message::Exit) {
                ASSERT(hdr.clientID() == 0);

                eckit::Log::status() << "Terminating CONTROL listener" << std::endl;
                eckit::Log::info() << "Terminating CONTROL listener" << std::endl;

                break;
            }
            else {

                Handled handled = Handled::No;
                ASSERT(single_ || hdr.control());

                if (payload.size()) {
                    if (hdr.control()) {
                        handled = handleControl(hdr.message, hdr.clientID(), hdr.requestID, std::move(payload));
                    }
                    else {
                        handled = handleData(hdr.message, hdr.clientID(), hdr.requestID, std::move(payload));
                    }
                }
                else {
                    if (hdr.control()) {
                        handled = handleControl(hdr.message, hdr.clientID(), hdr.requestID);
                    }
                    else {
                        handled = handleData(hdr.message, hdr.clientID(), hdr.requestID);
                    }
                }

                switch (handled) {
                    case Handled::Replied:  // nothing to do
                        break;
                    case Handled::YesAddArchiveListener:
                    case Handled::YesAddReadListener: {
                        std::lock_guard<std::mutex> lock(handlerMutex_);
                        numDataListener_++;
                        if (numDataListener_ == 1 && !single_) {
                            listeningThreadData = std::thread([this] { listeningThreadLoopData(); });
                        }
                    }
                    case Handled::Yes:
                        write(Message::Received, true, hdr.clientID(), hdr.requestID);
                        break;
                    case Handled::No:
                    default:
                        std::stringstream ss;
                        ss << "Unable to handle message " << hdr.message;
                        error(ss.str(), hdr.clientID(), hdr.requestID);
                }
            }
        }
    }

    if (listeningThreadData.joinable()) {
        listeningThreadData.join();
    }
    ASSERT(archiveQueue_.empty());
    archiveQueue_.close();

    teardown();
}

void ServerConnection::handleException(std::exception_ptr e) {
    try {
        if (e)
            std::rethrow_exception(e);
    }
    catch (const std::exception& e) {
        error(e.what(), 0, 0);
    }
}

void ServerConnection::queue(Message message, uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {

    archiveQueue_.emplace(ArchiveElem{clientID, requestID, std::move(payload), message == Message::MultiBlob});
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

void ServerConnection::archiver() {

    // Ensure that we aren't already running a catalogue/store
    if (!archiveFuture_.valid()) {
        // Start archive worker thread
        archiveFuture_ = std::async(std::launch::async, [this] { return archiveThreadLoop(); });
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

    std::lock_guard<std::mutex> lock(readLocationMutex_);
    if (readLocationWorker_.joinable()) {
        readLocationWorker_.join();
    }
}

}  // namespace fdb5::remote
