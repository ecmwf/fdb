// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#include "fdb5/remote/server/ServerConnection.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/fdb5_version.h"
#include "fdb5/remote/Connection.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/remote/server/AvailablePortList.h"

#include "eckit/config/LocalConfiguration.h"
#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/log/CodeLocation.h"
#include "eckit/log/Log.h"
#include "eckit/net/Endpoint.h"
#include "eckit/net/IPAddress.h"
#include "eckit/net/NetMask.h"
#include "eckit/net/TCPServer.h"
#include "eckit/net/TCPSocket.h"
#include "eckit/runtime/SessionID.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/utils/Literals.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <future>
#include <iterator>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
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
constexpr const auto defaultArchiveQueueSize = 320;

// Largest message accepted before the handshake has completed (the startup message is a few hundred bytes)
constexpr uint32_t maxStartupPayloadSize = 64 * 1024;

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

ServerConnection::ServerConnection(eckit::net::TCPSocket& socket, const Config& config) :
    config_(config),
    dataListenHostname_(config.getString("dataListenHostname", "")),
    readLocationQueue_(eckit::Resource<size_t>("fdbRetrieveQueueSize", defaultRetrieveQueueSize)),
    archiveQueue_(eckit::Resource<size_t>("fdbServerMaxQueueSize", defaultArchiveQueueSize)),
    controlSocket_(socket) {

    innerConfig_ = config_.has("fdb") ? Config{config_.getSubConfiguration("fdb"), config_.userConfig()} : config_;

    // Until the client is identified, anybody who can connect is talking: only the startup message is expected
    maxPayloadSize(maxStartupPayloadSize);

    if (config_.has("networks")) {
        const eckit::net::IPAddress clientAddress{controlSocket_.remoteAddr()};
        const auto networks = config_.getSubConfigurations("networks");

        // The first network containing the address of the client is its network
        std::optional<eckit::LocalConfiguration> clientNetwork;
        for (const auto& net : networks) {
            if (net.has("name") && net.has("netmask") &&
                eckit::net::NetMask{net.getString("netmask")}.contains(clientAddress)) {
                clientNetwork_ = net.getString("name");
                clientNetwork = net;
                break;
            }
        }

        // A feature that some network sets (e.g. "wipe: true") is restricted: it is granted only to the clients of the
        // networks setting it to true. Clients of other networks, or of no network, don't get it.
        for (const auto& [name, message] : networkRestrictableFeatures()) {
            const bool restricted = std::any_of(networks.begin(), networks.end(),
                                                [&name = name](const auto& net) { return net.has(name); });
            const bool granted = clientNetwork && clientNetwork->has(name) && clientNetwork->getBool(name);
            if (restricted && !granted) {
                disabledFeatures_ |= toMask(message);
            }
        }

        LOG_DEBUG_LIB(LibFdb5) << "Client " << clientAddress << " from network '" << clientNetwork_
                               << "', disabled features: " << messageMask2String(disabledFeatures_) << std::endl;
    }

    LOG_DEBUG_LIB(LibFdb5) << "ServerConnection::ServerConnection initialized" << std::endl;
}

ServerConnection::~ServerConnection() {
    // The derived handler has already been destroyed: a worker still running would use its state, or call its
    // methods (e.g. archiveBlob()) as pure virtual. Stopping the workers here would not make that safe: they must have
    // been stopped by handle(), or by the destructor of the derived handler (see stopWorkers()). A worker still
    // running is a bug of the derived handler: fail loudly rather than with a use-after-free.
    if (workersRunning()) {
        eckit::Log::error() << "ServerConnection destroyed with worker threads still running: the derived handler must "
                               "call stopWorkers() in its destructor"
                            << std::endl;
        std::terminate();
    }

    eckit::Log::info() << "Done" << std::endl;
}

bool ServerConnection::workersRunning() noexcept {
    const auto running = [](const auto& future) {
        return future.valid() && future.wait_for(std::chrono::seconds(0)) != std::future_status::ready;
    };

    if (running(archiveFuture_)) {
        return true;
    }
    for (const auto& [requestID, worker] : workerThreads_) {
        if (running(worker)) {
            return true;
        }
    }
    try {
        std::lock_guard<std::mutex> lock(readLocationMutex_);
        // n.b. even once finished, a thread that has not been joined would terminate the process when destroyed
        return readLocationWorker_.joinable();
    }
    catch (...) {
        return true;
    }
}

//----------------------------------------------------------------------------------------------------------------------

Handled ServerConnection::handleData(Message message, uint32_t clientID, uint32_t requestID) {
    try {
        switch (message) {
            case Message::Flush: {
                // Flush is a control message: on the data connection it would stop the data listener, which is only
                // stopped by the end of the connection
                throw RemoteProtocolException("Flush received on the data connection", Here());
            }

            default: {
                std::ostringstream ss;
                ss << "ERROR: Unexpected message recieved (" << message << "). ABORTING";
                eckit::Log::status() << ss.str() << std::endl;
                eckit::Log::error() << ss.str() << std::endl;
                throw eckit::SeriousBug(ss.str(), Here());
            }
        }
    }
    catch (std::exception& e) {
        // n.b. more general than eckit::Exception
        error(false, e.what(), clientID, requestID);
    }
    catch (...) {
        error(false, "Caught unexpected and unknown error", clientID, requestID);
    }
    return Handled::Replied;
}

Handled ServerConnection::handleData(Message message, uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {
    try {
        switch (message) {
            case Message::Blob:
            case Message::MultiBlob:
                queue(message, clientID, requestID, std::move(payload));
                return Handled::Replied;

            default: {
                std::ostringstream ss;
                ss << "ERROR: Unexpected message recieved (" << message << "). ABORTING";
                eckit::Log::status() << ss.str() << std::endl;
                eckit::Log::error() << ss.str() << std::endl;
                throw eckit::SeriousBug(ss.str(), Here());
            }
        }
    }
    catch (std::exception& e) {
        // An interrupted queue means that the archive worker has failed: it owns the report of that failure, this
        // is only a consequence of it. Let the listener stop instead.
        if (connectionFailed()) {
            throw;
        }
        // n.b. more general than eckit::Exception
        error(false, e.what(), clientID, requestID);
    }
    catch (...) {
        if (connectionFailed()) {
            throw;
        }
        error(false, "Caught unexpected and unknown error", clientID, requestID);
    }
    return Handled::Replied;
}

const std::vector<std::pair<std::string, Message>>& ServerConnection::networkRestrictableFeatures() {
    // the name of the setting (also in the configuration of the server, to disable the feature for all) and the feature
    static const std::vector<std::pair<std::string, Message>> features{{"wipe", Message::Wipe}};
    return features;
}

RemoteConfiguration ServerConnection::availableFunctionality() const {
    RemoteConfiguration conf{config_};
    // n.b. not part of the configuration: it depends on the network of the client (see the constructor)
    conf.disableFeatures(disabledFeatures_);
    return conf;
}

bool ServerConnection::initialiseConnections() {
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
        error(true, "Error retrieving client protocol version", hdr.clientID(), hdr.requestID);
        return false;
    }

    if (!LibFdb5::instance().remoteProtocolVersion().check(remoteProtocolVersion, false)) {
        std::ostringstream ss;
        ss << "FDB server version " << fdb5_version_str() << " - remote protocol version not supported:" << std::endl;
        ss << "    versions supported by server: " << LibFdb5::instance().remoteProtocolVersion().supportedStr()
           << std::endl;
        ss << "    version requested by client: " << remoteProtocolVersion << std::endl;
        error(true, ss.str(), hdr.clientID(), hdr.requestID);
        return false;
    }

    RemoteConfiguration clientConf{s1};
    RemoteConfiguration serverConf = availableFunctionality();
    try {
        agreedConf_ = RemoteConfiguration::common(clientConf, serverConf);
        single_ = agreedConf_.singleConnection();
    }
    catch (const eckit::Exception& e) {
        error(true, e.what(), hdr.clientID(), hdr.requestID);
        return false;
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

        // A client that never connects must not hold this connection (and its data port) forever
        const int timeoutSeconds = eckit::Resource<int>("fdbDataConnectionTimeout;$FDB_DATA_CONNECTION_TIMEOUT", 60);
        dataSocketFuture = std::async(std::launch::async, [this, timeoutSeconds] {
            dataSocket_->accept("Waiting for the data connection of the client", timeoutSeconds);
            return true;
        });
    }

    eckit::Buffer startupBuffer(1_KiB);
    eckit::MemoryStream s(startupBuffer);

    s << clientSession;
    s << sessionID_;
    s << dataEndpoint;
    s << agreedConf_;

    write(Message::Startup, true, 0, 0, startupBuffer.data(), s.position());

    if (!single_) {
        ASSERT(dataSocketFuture.valid());
        static_cast<void>(dataSocketFuture.get());  // n.b. rethrows a timeout: the connection fails

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
            std::ostringstream ss;
            ss << "Client session IDs do not match: " << clientSession << " != " << clientSession2;
            throw eckit::BadValue(ss.str(), Here());
        }

        if (serverSession != sessionID_) {
            std::ostringstream ss;
            ss << "Session IDs do not match: " << serverSession << " != " << sessionID_;
            throw eckit::BadValue(ss.str(), Here());
        }
    }

    return true;
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
        while (archiveQueue_.pop(elem) != -1) {
            // n.b. only once it is processed: a flush waiting for locations relies on it (see archivePending())
            struct Processed {
                std::atomic<size_t>& pending;
                ~Processed() { --pending; }
            } processed{archivePending_};

            if (discardArchive_) {
                continue;
            }

            try {
                if (elem.multiblob_) {
                    // Handle MultiBlob

                    // A sequence of complete Blob frames. The content comes from the client: every size is checked
                    // against what is left in the buffer before it is used (a failure aborts the connection, see below)
                    const char* data = static_cast<const char*>(elem.payload_.data());
                    const size_t size = elem.payload_.size();
                    size_t offset = 0;
                    while (offset < size) {
                        if (size - offset < sizeof(MessageHeader)) {
                            throw RemoteProtocolException("truncated message header in MultiBlob", Here());
                        }
                        MessageHeader hdr;
                        // n.b. may not be aligned; copied raw, as read() does
                        std::memcpy(static_cast<void*>(&hdr), data + offset, sizeof(hdr));
                        offset += sizeof(MessageHeader);

                        if (!(hdr.marker == MessageHeader::StartMarker) || hdr.message != Message::Blob ||
                            hdr.clientID() != elem.clientID_ || hdr.requestID != elem.requestID_) {
                            std::ostringstream ss;
                            ss << "invalid frame in MultiBlob [message=" << hdr.message
                               << ",clientID=" << hdr.clientID() << ",requestID=" << hdr.requestID
                               << "], expected Blob for [clientID=" << elem.clientID_
                               << ",requestID=" << elem.requestID_ << "]";
                            throw RemoteProtocolException(ss.str(), Here());
                        }
                        if (size - offset < MessageHeader::markerBytes ||
                            hdr.payloadSize > size - offset - MessageHeader::markerBytes) {
                            std::ostringstream ss;
                            ss << "payload of " << hdr.payloadSize << " bytes overruns the MultiBlob of " << size
                               << " bytes";
                            throw RemoteProtocolException(ss.str(), Here());
                        }

                        const void* payloadData = data + offset;
                        offset += hdr.payloadSize;

                        if (std::memcmp(data + offset, MessageHeader::EndMarker.data(), MessageHeader::markerBytes) !=
                            0) {
                            throw RemoteProtocolException("invalid end of frame in MultiBlob", Here());
                        }
                        offset += MessageHeader::markerBytes;

                        archiveBlob(elem.clientID_, elem.requestID_, payloadData, hdr.payloadSize);
                        totalArchived += 1;
                    }
                }
                else {
                    // Handle single blob
                    archiveBlob(elem.clientID_, elem.requestID_, elem.payload_.data(), elem.payload_.size());
                    totalArchived += 1;
                }
            }
            catch (...) {
                // Published before this element is counted as processed (when the guard above is destroyed): a flush
                // that sees no pending work must also see the failure, not report missing locations
                recordArchiveFailure(std::current_exception());
                throw;
            }
        }
    }
    catch (...) {
        const auto eptr = std::current_exception();

        std::string what = "Caught unexpected, unknown exception in archive worker";
        try {
            std::rethrow_exception(eptr);
        }
        catch (const std::exception& e) {
            what = e.what();
        }
        catch (...) {
        }

        // Record the failure before anybody is told about it, so that a flush can never miss it (n.b. already done
        // for a failure while processing an element)
        recordArchiveFailure(eptr);

        // The worker is shared, so all pending requests must be failed without waiting for a socket writer.
        eckit::Log::error() << "Archive worker failed [clientID=" << elem.clientID_ << ",requestID=" << elem.requestID_
                            << "], terminating the connection: " << what << std::endl;

        // This worker detected the failure, so it owns the error-send-and-abort sequence. It must complete it before
        // interrupting the queue: the interruption wakes up the data listener (and, on a single connection, the
        // control listener) blocked on a full queue, which would otherwise race to report the interruption as their
        // own connection-level failure and shut down the sockets before the specific error is sent.
        // The failing request gets the actual reason; the others only see the connection being lost.
        failConnection(what, elem.clientID_, elem.requestID_);

        // Ensure exception propagates across the queue back to the parent thread.
        archiveQueue_.interrupt(eptr);

        throw;
    }

    return totalArchived;
}

void ServerConnection::listeningThreadLoopData() {

    // however it ends, no more archive data can arrive (see archiveDataEnded())
    struct Ended {
        std::atomic<bool>& ended;
        ~Ended() { ended = true; }
    } ended{dataEnded_};

    MessageHeader hdr{};

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

                // n.b. the data listener runs until the end of the connection: once started, it is never restarted
                switch (handled) {
                    case Handled::Replied:  // nothing to do
                    case Handled::Yes:
                        break;
                    case Handled::No:
                    default:
                        std::ostringstream ss;
                        ss << "Unable to handle message " << hdr.message << " received on the data connection";
                        error(false, ss.str(), hdr.clientID(), hdr.requestID);
                }
            }
        }
    }
    // Rethrowing from a std::thread would std::terminate the whole server, so report and exit the loop instead.
    // A failure to read the data connection means that the stream is out of sync (or gone): nothing more can be
    // received on it, so the whole connection is terminated to wake up whoever is waiting for it.
    // If another thread already owns a failure (e.g. the archive worker interrupted the queue), this is only a
    // consequence of it: failConnection() then leaves the reporting to that thread.
    catch (std::exception& e) {
        // n.b. more general than eckit::Exception
        if (failConnection(std::string("Data listener failed: ") + e.what(), 0, 0)) {
            eckit::Log::error() << "Data listener failed, terminated the connection: " << e.what() << std::endl;
        }
    }
    catch (...) {
        if (failConnection("Data listener failed with an unknown exception", 0, 0)) {
            eckit::Log::error() << "Data listener failed, terminated the connection" << std::endl;
        }
    }
}

void ServerConnection::listenControl(std::thread& listeningThreadData) {
    if (!initialiseConnections()) {
        // the client has been told why it was rejected, and will disconnect
        eckit::Log::warning() << "Client rejected during the startup handshake" << std::endl;
        return;
    }

    // n.b. archived fields are sent as a single message
    static const auto maxPayload = static_cast<uint32_t>(
        std::min<size_t>(eckit::Resource<size_t>("fdbServerMaxPayloadSize;$FDB_SERVER_MAX_PAYLOAD_SIZE",
                                                 std::numeric_limits<uint32_t>::max()),
                         std::numeric_limits<uint32_t>::max()));
    maxPayloadSize(maxPayload);

    MessageHeader hdr{};

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
                        if (handled == Handled::YesAddReadListener) {
                            break;  // StoreHandler::read() already sent the acknowledgement
                        }
                    }
                        [[fallthrough]];
                    case Handled::Yes:
                        write(Message::Received, true, hdr.clientID(), hdr.requestID);
                        break;
                    case Handled::No:
                    default:
                        std::ostringstream ss;
                        ss << "Unable to handle message " << hdr.message;
                        error(true, ss.str(), hdr.clientID(), hdr.requestID);
                }
            }
        }
    }
}

void ServerConnection::handle() {
    std::thread listeningThreadData;
    std::string failure;

    // Nothing may escape from here: an exception unwinding with a joinable listeningThreadData would terminate the
    // process, and the workers must be stopped while the derived handler is still alive.
    try {
        listenControl(listeningThreadData);

        if (listeningThreadData.joinable()) {
            listeningThreadData.join();
        }

        // if the connection was aborted by a worker, whatever is still queued is discarded
        if (!connectionFailed()) {
            ASSERT(archiveQueue_.empty());
        }
    }
    catch (const std::exception& e) {
        failure = e.what();
    }
    catch (...) {
        failure = "Caught unexpected, unknown exception in connection handler";
    }

    if (!failure.empty()) {
        eckit::Log::error() << "Terminating connection: " << failure << std::endl;

        // Shut down without waiting for a socket writer so the data listener can be joined.
        // The error is not addressed to a request or a client: the client reports it as a connection failure.
        // n.b. if a worker owns the failure that caused this one, it is the worker that reports it.
        failConnection(failure, 0, 0);
        try {
            if (listeningThreadData.joinable()) {
                listeningThreadData.join();
            }
        }
        catch (...) {
        }
    }

    // The connection is also aborted by the data listener and by the archive worker, on their own failures.
    // n.b. a worker may own a failure without having aborted the connection yet: stopWorkers() waits for it.
    const bool failed = connectionFailed();

    stopWorkers(failed);

    if (!failed) {
        teardown();
    }
}

void ServerConnection::queue(Message message, uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {

    ++archivePending_;  // before it can be processed
    try {
        archiveQueue_.emplace(ArchiveElem{clientID, requestID, std::move(payload), message == Message::MultiBlob});
    }
    catch (...) {
        --archivePending_;
        throw;
    }
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
        // Called from the destructor: an exception escaping here would terminate the process
        try {
            it.second.get();
        }
        catch (const std::exception& e) {
            eckit::Log::error() << "Worker thread for request ID " << it.first << " failed: " << e.what() << std::endl;
        }
        catch (...) {
            eckit::Log::error() << "Worker thread for request ID " << it.first << " failed with an unknown exception"
                                << std::endl;
        }
        eckit::Log::error() << "Thread complete" << std::endl;
    }

    workerThreads_.clear();

    std::lock_guard<std::mutex> lock(readLocationMutex_);
    if (readLocationWorker_.joinable()) {
        readLocationWorker_.join();
    }
}

void ServerConnection::stopWorkers(bool discardPending) noexcept {
    try {
        discardArchive_ = discardPending;
        archiveQueue_.close();  // the archive worker still empties the queue, discarding the elements if requested

        if (archiveFuture_.valid()) {
            archiveFuture_.wait();  // wait() rather than get(): the failure has already been reported
        }

        waitForWorkers();
    }
    catch (...) {
        eckit::Log::error() << "Unexpected error while stopping the workers" << std::endl;
    }
}

bool ServerConnection::failConnection(std::string_view what, uint32_t clientID, uint32_t requestID) noexcept {
    if (failureOwned_.exchange(true)) {
        return false;
    }
    tryErrorBounded(true, what, clientID, requestID);
    abortConnection();
    return true;
}

void ServerConnection::recordArchiveFailure(const std::exception_ptr& failure) noexcept {
    try {
        std::lock_guard<std::mutex> lock(archiveFailureMutex_);
        if (!archiveFailure_) {
            archiveFailure_ = failure;
        }
    }
    catch (...) {
    }
}

void ServerConnection::rethrowArchiveFailure() const {
    std::lock_guard<std::mutex> lock(archiveFailureMutex_);
    if (archiveFailure_) {
        std::rethrow_exception(archiveFailure_);
    }
}

}  // namespace fdb5::remote
