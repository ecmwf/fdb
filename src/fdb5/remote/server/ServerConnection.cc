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
#include "fdb5/tracing/TraceContext.h"
#include "fdb5/tracing/Tracer.h"

#include "eckit/config/LocalConfiguration.h"
#include "eckit/config/Resource.h"
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

/// The trace context announced by a Message::Trace frame for the next frame received on one channel.
///
/// Tracing must never disrupt data flow: a Trace frame that was not negotiated, cannot be decoded, or does not match
/// the frame that follows it is reported and ignored, without failing the request.
class IncomingTrace {
public:

    void set(const MessageHeader& hdr, const eckit::Buffer& payload, int tracingVersion) {
        pending_ = false;
        if (tracingVersion == 0) {
            eckit::Log::warning() << "Ignoring " << Message::Trace << " frame from clientID=" << hdr.clientID()
                                  << ": trace-context propagation was not negotiated" << std::endl;
            return;
        }
        try {
            context_ = tracing::TraceContext::decode(payload.data(), hdr.payloadSize);
        }
        catch (const eckit::Exception& e) {
            eckit::Log::warning() << "Ignoring malformed " << Message::Trace
                                  << " frame from clientID=" << hdr.clientID() << ": " << e.what() << std::endl;
            return;
        }
        clientID_ = hdr.clientID();
        requestID_ = hdr.requestID;
        pending_ = true;
    }

    /// The context of @p hdr (invalid if none was announced). Consumes the pending context.
    tracing::TraceContext take(const MessageHeader& hdr) {
        if (!pending_) {
            return {};
        }
        pending_ = false;
        if (hdr.clientID() != clientID_ || hdr.requestID != requestID_) {
            eckit::Log::warning() << "Ignoring " << Message::Trace << " frame for [clientID=" << clientID_
                                  << ",requestID=" << requestID_ << "] followed by " << hdr.message
                                  << " [clientID=" << hdr.clientID() << ",requestID=" << hdr.requestID << "]"
                                  << std::endl;
            return {};
        }
        return context_;
    }

private:

    bool pending_{false};
    uint32_t clientID_{0};
    uint32_t requestID_{0};
    tracing::TraceContext context_;
};

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

ServerConnection::ServerConnection(eckit::net::TCPSocket& socket, const Config& config) :
    config_(config),
    tracing_(tracing::Tracer::enabledIn(config)),
    dataListenHostname_(config.getString("dataListenHostname", "")),
    readLocationQueue_(eckit::Resource<size_t>("fdbRetrieveQueueSize", defaultRetrieveQueueSize)),
    archiveQueue_(eckit::Resource<size_t>("fdbServerMaxQueueSize", defaultArchiveQueueSize)),
    controlSocket_(socket) {

    innerConfig_ = config_.has("fdb") ? Config{config_.getSubConfiguration("fdb"), config_.userConfig()} : config_;

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
        // n.b. more general than eckit::Exception
        error(false, e.what(), clientID, requestID);
    }
    catch (...) {
        error(false, "Caught unexpected and unknown error", clientID, requestID);
    }
    return Handled::Replied;
}

RemoteConfiguration ServerConnection::availableFunctionality() const {
    return RemoteConfiguration{config_};
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
        error(true, "Error retrieving client protocol version", hdr.clientID(), hdr.requestID);
        return;
    }

    if (!LibFdb5::instance().remoteProtocolVersion().check(remoteProtocolVersion, false)) {
        std::ostringstream ss;
        ss << "FDB server version " << fdb5_version_str() << " - remote protocol version not supported:" << std::endl;
        ss << "    versions supported by server: " << LibFdb5::instance().remoteProtocolVersion().supportedStr()
           << std::endl;
        ss << "    version requested by client: " << remoteProtocolVersion << std::endl;
        error(true, ss.str(), hdr.clientID(), hdr.requestID);
        return;
    }

    RemoteConfiguration clientConf{s1};
    RemoteConfiguration serverConf = availableFunctionality();
    try {
        agreedConf_ = RemoteConfiguration::common(clientConf, serverConf);
        single_ = agreedConf_.singleConnection();
        tracingVersion_ = agreedConf_.tracingVersion();
    }
    catch (const eckit::Exception& e) {
        error(true, e.what(), hdr.clientID(), hdr.requestID);
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
    s << agreedConf_;

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
            std::optional<tracing::ScopedSpan> span;
            if (tracing_) {
                span.emplace("fdb.server.archive", tracing::SpanKind::Server, elem.trace_);
                span->setAttribute("fdb.bytes", elem.payload_.size());
            }

            if (elem.multiblob_) {
                // Handle MultiBlob

                const char* firstData = reinterpret_cast<const char*>(elem.payload_.data());  // For pointer arithmetic
                const char* charData = firstData;
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
    IncomingTrace incomingTrace;

    try {

        // The archive loop is the only thing that can listen on the data socket,
        // so we don't need to to anything clever here.
        // n.b. we also don't need to lock on read. We are the only thing that reads.
        while (true) {
            eckit::Buffer payload = readData(hdr);  // READ DATA

            if (hdr.message == Message::Trace) {
                incomingTrace.set(hdr, payload, tracingVersion_);
                continue;
            }
            // Work queued while handling this frame (e.g. blobs for the archiver) captures this context
            tracing::ScopedContext traceScope(incomingTrace.take(hdr));

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
                        std::ostringstream ss;
                        ss << "Unable to handle message " << hdr.message << " received on the data connection";
                        error(false, ss.str(), hdr.clientID(), hdr.requestID);
                }
            }
        }
    }
    catch (std::exception& e) {
        // n.b. more general than eckit::Exception
        error(false, e.what(), hdr.clientID(), hdr.requestID);
        throw;
    }
    catch (...) {
        error(false, "Caught unexpected, unknown exception in retrieve worker", hdr.clientID(), hdr.requestID);
        throw;
    }
}

void ServerConnection::handle() {
    initialiseConnections();

    std::thread listeningThreadData;

    MessageHeader hdr;
    IncomingTrace incomingTrace;

    // listen loop
    while (true) {
        eckit::Buffer payload = readControl(hdr);  // READ CONTROL
        LOG_DEBUG_LIB(LibFdb5) << "ServerConnection::handle - got [message=" << hdr.message
                               << ",clientID=" << hdr.clientID() << ",requestID=" << hdr.requestID
                               << ",payload=" << hdr.payloadSize << "]" << std::endl;

        if (hdr.message == Message::Trace) {
            incomingTrace.set(hdr, payload, tracingVersion_);
            continue;
        }

        // The client's context is current while handling the message, so that work handed to worker threads
        // inherits it; synchronously handled requests are recorded as a server span
        tracing::ScopedContext traceScope(incomingTrace.take(hdr));
        std::optional<tracing::ScopedSpan> span;
        if (tracing_ && hdr.control()) {
            if (const char* name = serverSpanName(hdr.message)) {
                span.emplace(name, tracing::SpanKind::Server, tracing::current());
            }
        }

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
                        if (span) {
                            span->span().setStatus(tracing::StatusCode::Error, ss.str());
                        }
                        error(true, ss.str(), hdr.clientID(), hdr.requestID);
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

const char* ServerConnection::serverSpanName(Message message) const {
    switch (message) {
        case Message::Flush:
            return "fdb.server.flush";
        case Message::Exists:
            return "fdb.server.exists";
        case Message::Schema:
            return "fdb.server.schema";
        case Message::Stores:
            return "fdb.server.stores";
        case Message::Wipe:
            return "fdb.server.wipe";
        case Message::DoWipeURIs:
            return "fdb.server.wipe.uris";
        case Message::DoWipeUnknowns:
            return "fdb.server.wipe.unknowns";
        case Message::DoWipeFinish:
            return "fdb.server.wipe.finish";
        case Message::DoMaskIndexEntries:
            return "fdb.server.wipe.mask_index_entries";
        case Message::DoUnsafeFullWipe:
            return "fdb.server.wipe.unsafe_full";
        default:
            // Archive/Store open a stream whose blobs are traced by the archiver thread; Read and the iterating
            // API calls are traced by their worker threads
            return nullptr;
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
