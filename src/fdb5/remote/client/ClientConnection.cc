
#include "fdb5/remote/client/ClientConnection.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/remote/Connection.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/remote/RemoteConfiguration.h"
#include "fdb5/remote/client/Client.h"
#include "fdb5/remote/client/ClientConnectionRouter.h"

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

#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <future>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace eckit;
using namespace eckit::literals;

namespace fdb5::remote {

namespace {
std::string msgHeader(MessageHeader& hdr, net::Endpoint& endpoint) {
    std::ostringstream ss;
    ss << (hdr.control() ? "CONTROL" : "DATA") << " connection=" << endpoint << " [message=" << hdr.message
       << ",clientID=" << hdr.clientID() << ",requestID=" << hdr.requestID << ",payload=" << hdr.payloadSize << "]";
    return ss.str();
}

std::string errorText(const Buffer& payload) {
    return std::string(static_cast<const char*>(payload.data()), payload.size());
}
}  // namespace

//----------------------------------------------------------------------------------------------------------------------

class DataWriteRequest {

public:

    DataWriteRequest() = default;

    DataWriteRequest(uint32_t clientID, Message msg, uint32_t id, Buffer&& data) :
        clientID_(clientID), msg_(msg), id_(id), data_(std::move(data)) {}

    /// @param barrier: no payload, signals when all queued writes before this barrier have been processed
    explicit DataWriteRequest(std::shared_ptr<std::promise<void>> barrier) : barrier_{std::move(barrier)} {}

    uint32_t clientID_{0};
    Message msg_{Message::None};
    uint32_t id_{0};
    Buffer data_{Buffer(0)};
    std::shared_ptr<std::promise<void>> barrier_;
};

//----------------------------------------------------------------------------------------------------------------------

ClientConnection::ClientConnection(const net::Endpoint& controlEndpoint, const std::string& defaultEndpoint) :
    controlEndpoint_(controlEndpoint),
    defaultEndpoint_(defaultEndpoint),
    id_(1),
    connected_(false),
    dataWriteQueue_(nullptr) {

    LOG_DEBUG_LIB(LibFdb5) << "ClientConnection::ClientConnection() controlEndpoint: " << controlEndpoint << std::endl;
}

template <typename F>
bool ClientConnection::withClient(uint32_t clientID, F&& f, bool includeRetiring) {
    std::shared_ptr<ClientEntry> entry;
    {
        std::lock_guard lock(clientsMutex_);
        auto it = clients_.find(clientID);
        if (it == clients_.end() || (it->second->retiring && !includeRetiring)) {
            return false;
        }
        entry = it->second;
        ++entry->pins;
    }

    struct Unpin {
        ClientConnection& connection;
        ClientEntry& entry;
        ~Unpin() { connection.unpin(entry); }
    } unpin{*this, *entry};

    f(entry->client);
    return true;
}

bool ClientConnection::wasRemoved(uint32_t clientID) {
    std::lock_guard lock(clientsMutex_);
    if (removedClients_.count(clientID) != 0) {
        return true;
    }
    auto it = clients_.find(clientID);
    return it != clients_.end() && it->second->retiring;
}

void ClientConnection::unpin(ClientEntry& entry) {
    {
        std::lock_guard lock(clientsMutex_);
        ASSERT(entry.pins > 0);
        --entry.pins;
    }
    clientsCondition_.notify_all();
}

bool ClientConnection::add(Client& client) {
    std::lock_guard lock(clientsMutex_);
    // once its last client is gone the connection is retired (under this lock), and torn down: never reused
    if (!valid() || retired_) {
        return false;
    }
    clients_[client.id()] = std::make_shared<ClientEntry>(client);
    return true;
}

bool ClientConnection::remove(uint32_t clientID) {
    std::unique_lock lock(clientsMutex_);
    bool stop = false;

    if (clientID > 0) {

        auto it = clients_.find(clientID);

        if (it != clients_.end() && !it->second->retiring) {
            // No new message is dispatched to the client, but it stays registered, so that it is still told if the
            // connection is lost, until the calls in progress have completed: it can only be destroyed afterwards.
            const auto entry = it->second;
            entry->retiring = true;

            if (entry->pins > 0) {
                // A call in progress may be blocked, e.g. on the full queue of a read nobody consumes any more: cancel
                // the pending requests of the client, which is going away, so that the call completes.
                ++entry->pins;
                lock.unlock();
                try {
                    std::ostringstream ss;
                    ss << "Client " << clientID << " removed from the connection to " << controlEndpoint_;
                    entry->client.closeConnection(std::make_exception_ptr(RemoteFDBException(ss.str(), controlEndpoint_)));
                }
                catch (...) {
                    Log::error() << "ClientConnection::remove() - failed to cancel the requests of client " << clientID
                                 << std::endl;
                }
                lock.lock();
                --entry->pins;
            }

            clientsCondition_.wait(lock, [&entry] { return entry->pins == 0; });
            clients_.erase(clientID);
            removedClients_.insert(clientID);  // client IDs are unique in the process: 4 bytes per removed client
            stop = true;
        }
    }

    // The last client gone, the connection is retired: no client may be added any more (see add()), and it is torn
    // down below, without the lock
    const bool empty = clients_.empty();
    const bool last = empty && !retired_.exchange(true);
    lock.unlock();

    // No socket I/O with clientsMutex_ held: the listeners need it to dispatch, and a write may block while the
    // server is busy writing to us (e.g. on a single connection)
    if (stop && valid()) {
        try {
            Connection::write(Message::Stop, true, clientID, 0);
        }
        catch (...) {
            Log::error() << "ClientConnection::remove() - failed to send STOP message to server for clientID " << clientID
                         << std::endl;
        }
    }

    if (last) {
        teardown();
        ClientConnectionRouter::instance().deregister(*this);
    }

    return empty;
}

ClientConnection::~ClientConnection() {
    {
        std::lock_guard lock(dataWriteMutex_);
        if (dataWriteQueue_) {
            dataWriteQueue_->close();
        }
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

            runningListeners_ = 2;
            listeningDataThread_ = std::thread([this] { listeningDataThreadLoop(); });
        }
        else {
            runningListeners_ = 1;
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

void ClientConnection::checkEnabled(Message msg) const {
    if (!enabled(agreedFeatures_, msg)) {
        LOG_DEBUG_LIB(LibFdb5) << "Message " << msg << " not enabled - requested: " << messageMask2String(toMask(msg))
                               << ", enabled: " << messageMask2String(agreedFeatures_) << std::endl;

        // n.b. a permission error, not a failure: say which side does not allow it
        std::ostringstream ss;
        if (!enabled(offeredFeatures_, msg)) {
            ss << "'" << msg << "' is disabled in the client configuration (FDB server " << controlEndpoint_ << ")";
        }
        else {
            // e.g. disabled in the configuration of the server, or not granted to the network of this client
            ss << "'" << msg << "' is not permitted on FDB server " << controlEndpoint_
               << ": the server does not allow it for this client";
        }
        throw UserError(ss.str(), Here());
    }
}

std::future<Buffer> ClientConnection::controlWrite(const Client& client, const Message msg, const uint32_t requestID,
                                                   const bool /*dataListener*/, const PayloadList payloads) const {
    if (!valid()) {
        throw RemoteFDBException("Connection to " + std::string(controlEndpoint_) + " is no longer valid",
                                 controlEndpoint_);
    }

    checkEnabled(msg);

    std::future<Buffer> f;
    bool failed = false;
    {
        std::lock_guard lock(promisesMutex_);
        if (promisesFailed_) {
            failed = true;
        }
        else {
            auto pp = promises_.emplace(requestID, std::promise<Buffer>{}).first;
            f = pp->second.get_future();
        }
    }
    if (failed) {
        // The connection is failing, but may not be marked invalid yet (see handleConnectionError()): the request
        // would never be answered. n.b. the reason is recorded before the requests are failed.
        if (auto reason = connectionError()) {
            std::rethrow_exception(reason);
        }
        throw RemoteFDBException("Connection to " + std::string(controlEndpoint_) + " is no longer valid",
                                 controlEndpoint_);
    }

    try {
        Connection::write(msg, true, client.clientId(), requestID, payloads);
    }
    catch (...) {
        // not sent: nobody will answer it
        std::lock_guard lock(promisesMutex_);
        promises_.erase(requestID);
        throw;
    }

    return f;
}

void ClientConnection::dataWrite(DataWriteRequest& request) const {

    checkEnabled(request.msg_);

    Connection::write(request.msg_, false, request.clientID_, request.id_, request.data_.data(), request.data_.size());
}

void ClientConnection::dataWrite(Client& client, remote::Message msg, uint32_t requestID, PayloadList payloads) {

    static size_t maxQueueLength = Resource<size_t>("fdbDataWriteQueueLength;$FDB_DATA_WRITE_QUEUE_LENGTH", 320);

    {
        // retrieve or add client to the list
        std::lock_guard lock(clientsMutex_);
        ASSERT(clients_.find(client.clientId()) != clients_.end());
    }

    {
        std::lock_guard lock(dataWriteMutex_);
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

    dataWriteQueue_->emplace(client.clientId(), msg, requestID, std::move(buffer));
}

void ClientConnection::dataWriteThreadLoop() {

    Timer timer;
    DataWriteRequest element;

    auto* queue = dataWriteQueue_.get();
    ASSERT(queue);

    try {

        while (queue->pop(element) != -1) {
            if (element.barrier_) {
                std::lock_guard lock(dataWriteMutex_);
                element.barrier_->set_value();
                dataBarriers_.erase(std::remove(dataBarriers_.begin(), dataBarriers_.end(), element.barrier_),
                                    dataBarriers_.end());
                continue;
            }
            dataWrite(element);
        }

        std::lock_guard lock(dataWriteMutex_);
        dataWriteQueue_.reset();
    }
    catch (...) {
        // we must not rethrow in thread (that would std::terminate)
        // propagate and unblock any waiting flushDataWrites()
        queue->interrupt(std::current_exception());
        failDataBarriers(std::current_exception());
    }
}

void ClientConnection::flushDataWrites() {
    std::future<void> written;
    {
        std::lock_guard lock(dataWriteMutex_);
        if (!dataWriteQueue_) {
            return;
        }
        auto barrier = std::make_shared<std::promise<void>>();
        written = barrier->get_future();
        dataBarriers_.push_back(barrier);
        try {
            dataWriteQueue_->emplace(barrier);
        }
        catch (...) {
            LOG_DEBUG_LIB(LibFdb5) << "flushDataWrites - failed to enqueue barrier!" << std::endl;
            dataBarriers_.pop_back();
            throw;
        }
    }
    // block until the writer reaches the barrier (or the connection fails)
    written.get();
}

void ClientConnection::failDataBarriers(const std::exception_ptr& eptr) {
    std::lock_guard lock(dataWriteMutex_);
    for (auto& barrier : dataBarriers_) {
        try {
            barrier->set_exception(eptr);
        }
        catch (...) {
            Log::warning() << "failDataBarriers - barrier already satisfied." << std::endl;
        }
    }
    dataBarriers_.clear();
}

void ClientConnection::writeControlStartupMessage(const Configuration& config) {

    Buffer payload(4096);
    MemoryStream s(payload);
    s << sessionID_;
    s << net::Endpoint(controlEndpoint_.hostname(), controlEndpoint_.port());
    s << LibFdb5::instance().remoteProtocolVersion().used();

    // what this client allows: what the agreed features lack beyond it is refused by the server (see checkEnabled())
    const RemoteConfiguration offered = availableFunctionality(config);
    offeredFeatures_ = offered.enabledFeatures();
    s << offered;

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

Buffer ClientConnection::readStartupResponse(MessageHeader& hdr) {
    // A server that accepts the connection but never answers, or stops in the middle of its answer (overloaded,
    // wedged), must not block the caller forever: the whole answer is bounded, not only its first byte
    const int timeoutSeconds = Resource<int>("fdbStartupTimeout;$FDB_STARTUP_TIMEOUT", 60);
    const auto deadline = timeoutSeconds > 0 ? std::chrono::steady_clock::now() + std::chrono::seconds(timeoutSeconds)
                                             : std::chrono::steady_clock::time_point::max();

    // the answer is a few hundred bytes: until the server is identified, nothing larger is accepted
    constexpr uint32_t maxStartupPayloadSize = 64 * 1024;
    maxPayloadSize(maxStartupPayloadSize);
    struct Restore {
        ClientConnection& connection;
        ~Restore() { connection.maxPayloadSize(std::numeric_limits<uint32_t>::max()); }
    } restore{*this};

    try {
        return Connection::readControlBefore(hdr, deadline);
    }
    catch (const ConnectionTimeout&) {
        std::ostringstream ss;
        ss << "FDB server " << controlEndpoint_ << " did not answer the startup message within " << timeoutSeconds
           << " s (fdbStartupTimeout)";
        throw RemoteFDBTimeout(ss.str(), controlEndpoint_);
    }
}

SessionID ClientConnection::verifyServerStartupResponse() {

    MessageHeader hdr;
    Buffer payload = readStartupResponse(hdr);

    // The server rejects a client (e.g. unsupported protocol version) by sending the reason instead of the Startup
    // reply: report it, rather than failing to deserialize it as a Startup reply
    if (hdr.message == Message::Error || hdr.message == Message::Unauthorised) {
        std::ostringstream ss;
        ss << "FDB server " << controlEndpoint_ << " rejected the connection";
        if (payload.size() > 0) {
            ss << ": " << errorText(payload);
        }
        if (hdr.message == Message::Unauthorised) {
            ss << " (not authorised)";
        }
        throw RemoteFDBRejected(ss.str(), controlEndpoint_);
    }
    if (hdr.message != Message::Startup) {
        std::ostringstream ss;
        ss << "Unexpected reply to the startup message from FDB server " << controlEndpoint_ << ": " << hdr.message;
        if (hdr.message == Message::Exit) {
            ss << " (connection closed by the server)";
        }
        throw RemoteFDBException(ss.str(), controlEndpoint_);
    }

    ASSERT(hdr.requestID == 0);

    MemoryStream s(payload);
    SessionID clientSession(s);
    SessionID serverSession(s);
    net::Endpoint dataEndpoint(s);
    RemoteConfiguration agreedConf{s};

    dataEndpoint_ = dataEndpoint;

    LOG_DEBUG_LIB(LibFdb5) << "verifyServerStartupResponse - Received from server " << clientSession << " "
                           << serverSession << " " << agreedConf << " " << dataEndpoint << std::endl;
    if (dataEndpoint_.hostname() != controlEndpoint_.hostname()) {
        Log::warning() << "Data and control interface hostnames do not match. " << dataEndpoint_.hostname()
                       << " /= " << controlEndpoint_.hostname() << std::endl;
    }

    if (clientSession != sessionID_) {
        std::ostringstream ss;
        ss << "Session ID does not match session received from server: " << sessionID_ << " != " << clientSession;
        throw BadValue(ss.str(), Here());
    }
    single_ = agreedConf.singleConnection();
    agreedFeatures_ = agreedConf.enabledFeatures();

    if (single_ && !(dataEndpoint_ == controlEndpoint_)) {
        Log::warning() << "Returned control interface does not match. " << dataEndpoint_ << " /= " << controlEndpoint_
                       << std::endl;
    }

    return serverSession;
}

void ClientConnection::failPendingRequests(const std::exception_ptr& eptr) {
    std::lock_guard lock(promisesMutex_);
    promisesFailed_ = true;  // nobody will answer a request registered from now on
    for (auto& [requestID, promise] : promises_) {
        promise.set_exception(eptr);
    }
    promises_.clear();
}

void ClientConnection::handleConnectionError(const std::exception_ptr& eptr) {
    std::string reason;
    try {
        if (eptr) {
            std::rethrow_exception(eptr);
        }
    }
    catch (const std::exception& e) {
        reason = e.what();
        Log::error() << "error: " << reason << std::endl;
    }
    catch (...) {
        reason = "unknown exception";
        Log::error() << "error: unknown exception on connection " << controlEndpoint_ << std::endl;
    }

    // nothing else will ever fulfil a promise once the listening thread has stopped
    std::ostringstream ss;
    ss << "Connection to " << controlEndpoint_ << " lost: " << reason;
    const auto error = std::make_exception_ptr(RemoteFDBException(ss.str(), controlEndpoint_));

    {
        // the first failure is the most specific: the other listener only sees its consequences
        std::lock_guard lock(controlListenerMutex_);
        if (!connectionError_) {
            connectionError_ = error;
        }
    }

    failPendingRequests(error);

    // wake up anyone waiting on a request (reads, archive locations), nothing else will. They get the same reason as
    // the pending control requests, rather than a generic closure.
    closeConnection(closureReason());

    // Tell the server, if the connection is still valid (a failed read or write has already invalidated it), without
    // waiting for long: another writer may hold the socket, blocked because the server stopped reading, and only the
    // abort releases it. Then end the connection on both channels: a failure may concern only one of them, and the
    // listener of the other one would otherwise wait on a healthy socket forever, keeping the sockets open (they are
    // released when both listeners have stopped) and blocking the destruction of the connection. Its read now
    // returns as an Exit.
    // n.b. the data listener calls this only after waiting (bounded) for the error the server may have sent on the
    // control socket.
    endConnection();
}

bool ClientConnection::awaitControlListener(std::chrono::milliseconds timeout) {
    std::unique_lock lock(controlListenerMutex_);
    return controlListenerCondition_.wait_for(lock, timeout, [this] { return controlListenerDone_; });
}

void ClientConnection::listenerStopped() noexcept {
    if (--runningListeners_ == 0) {
        // nothing reads the sockets any more: the connection is over
        invalidate();
        closeSockets();
    }
}

void ClientConnection::listeningControlThreadLoop() {

    // Declared first: runs last, once the connection error (if any) has been handled
    struct Stopped {
        ClientConnection& connection;
        ~Stopped() { connection.listenerStopped(); }
    } stopped{*this};

    // Signals that nothing is left to be read from the control socket, once the connection error is handled too
    struct Done {
        ClientConnection& connection;
        ~Done() {
            {
                std::lock_guard lock(connection.controlListenerMutex_);
                connection.controlListenerDone_ = true;
            }
            connection.controlListenerCondition_.notify_all();
        }
    } done{*this};

    try {

        MessageHeader hdr;

        while (true) {

            Buffer payload = Connection::readControl(hdr);

            LOG_DEBUG_LIB(LibFdb5) << "ClientConnection::listeningControlThreadLoop - "
                                   << msgHeader(hdr, controlEndpoint_) << std::endl;

            if (hdr.message == Message::Exit) {
                LOG_DEBUG_LIB(LibFdb5) << "ClientConnection::listeningControlThreadLoop() -- Control thread stopping"
                                       << std::endl;
                // The server ended the connection (or we did): it is over on both channels, it must not be handed out
                // again, and no pending request will ever be answered
                invalidate();
                failPendingRequests(closureReason());
                if (single_) {
                    // there is no data listener to end the streamed requests (reads, lists): their messages would
                    // have come on this socket
                    closeConnection(closureReason());
                }
                return;
            }
            if (hdr.clientID()) {

                ASSERT(hdr.control() || single_);

                bool handled = false;
                if (hdr.control()) {
                    // Hold promisesMutex_ only for the promise lookup/erase (NOT across handle(); risk a deadlock.
                    std::lock_guard lock(promisesMutex_);

                    auto pp = promises_.find(hdr.requestID);
                    if (pp != promises_.end()) {
                        if (hdr.message == Message::Received) {
                            if (hdr.payloadSize == 0) {
                                pp->second.set_value(Buffer(0));
                            }
                            else {
                                pp->second.set_value(std::move(payload));
                            }
                        }
                        else {  // Message::Error or Message::Unauthorised - must contain a payload with the error
                                // message
                            if (hdr.payloadSize == 0) {
                                std::ostringstream ss;
                                ss << "Received " << hdr.message << " message with no payload from server";
                                pp->second.set_exception(
                                    std::make_exception_ptr(RemoteFDBException(ss.str(), controlEndpoint_)));
                            }
                            else {
                                const std::string msg = errorText(payload);

                                if (hdr.message == Message::Error) {
                                    pp->second.set_exception(
                                        std::make_exception_ptr(RemoteFDBException(msg, controlEndpoint_)));
                                }
                                else if (hdr.message == Message::Unauthorised) {
                                    pp->second.set_exception(
                                        std::make_exception_ptr(RemoteFDBUnauthorised(msg, controlEndpoint_)));
                                }
                                else {
                                    std::ostringstream ss;
                                    ss << "Received unexpected message " << hdr.message << " from server";
                                    pp->second.set_exception(
                                        std::make_exception_ptr(RemoteFDBException(ss.str(), controlEndpoint_)));
                                }
                            }
                        }
                        promises_.erase(pp);
                        handled = true;
                    }
                }
                if (!handled) {  // not the answer to a blocking request (a promise), so dispatch to the client
                    // The client cannot be removed while it handles the message (see withClient())
                    const bool found = withClient(hdr.clientID(), [&](Client& client) {
                        if (hdr.payloadSize == 0) {
                            handled = client.handle(hdr.message, hdr.requestID);
                        }
                        else {
                            handled = client.handle(hdr.message, hdr.requestID, std::move(payload));
                        }
                    });
                    if (!found && wasRemoved(hdr.clientID())) {
                        // the client went away while the server was still sending: nobody will read it
                        LOG_DEBUG_LIB(LibFdb5) << "Discarding " << msgHeader(hdr, controlEndpoint_) << " for a removed client"
                                               << std::endl;
                        continue;
                    }
                    if (!found) {
                        std::ostringstream ss;
                        ss << "ERROR: " << msgHeader(hdr, controlEndpoint_) << " - ClientID not found. ABORTING";
                        Log::status() << ss.str() << std::endl;
                        Log::error() << "Retrieving... " << ss.str() << std::endl;
                        throw SeriousBug(ss.str(), Here());
                    }
                }

                if (!handled) {
                    std::ostringstream ss;
                    ss << "ERROR: " << msgHeader(hdr, controlEndpoint_);
                    if (hdr.message == Message::Error) {
                        ss << " - received an unhandled error";
                        if (hdr.payloadSize) {
                            ss << ": " << errorText(payload);
                        }
                        throw RemoteFDBException(ss.str(), controlEndpoint_);
                    }
                    ss << " - received unexpected message. ABORTING";
                    Log::status() << ss.str() << std::endl;
                    Log::error() << "Client Retrieving... " << ss.str() << std::endl;
                    throw SeriousBug(ss.str(), Here());
                }
            }
            else if (hdr.message == Message::Error) {
                // not addressed to any client, so it cannot be delivered to a request
                throw RemoteFDBException("Received an error from server: " + errorText(payload), controlEndpoint_);
            }
        }

        // We don't want to let exceptions escape inside a worker thread.
    }
    catch (...) {
        handleConnectionError(std::current_exception());
    }
}

std::exception_ptr ClientConnection::connectionError() const {
    std::lock_guard lock(controlListenerMutex_);
    return connectionError_;
}

std::exception_ptr ClientConnection::closureReason() {
    {
        std::lock_guard lock(controlListenerMutex_);
        if (connectionError_) {
            return connectionError_;
        }
    }
    return std::make_exception_ptr(
        RemoteFDBException("Connection to " + std::string(controlEndpoint_) + " closed", controlEndpoint_));
}

void ClientConnection::closeConnection(const std::exception_ptr& reason) {
    LOG_DEBUG_LIB(LibFdb5) << "ClientConnection::closeConnection() -- Data thread stopping" << std::endl;

    // The clients are not called with clientsMutex_ held: a listener may be blocked in a client (e.g. on the full
    // queue of a read), and only closeConnection() can wake it up
    std::vector<uint32_t> ids;
    {
        std::lock_guard lock(clientsMutex_);
        ids.reserve(clients_.size());
        for (const auto& [id, entry] : clients_) {
            ids.push_back(id);
        }
    }

    for (const auto id : ids) {
        try {
            // retiring clients too: one of their calls may be blocked until their requests are interrupted
            withClient(id, [&reason](Client& client) { client.closeConnection(reason); }, true);
        }
        catch (...) {
            Log::error() << "ClientConnection::closeConnection() - failed to notify client " << id << std::endl;
        }
    }
}

void ClientConnection::listeningDataThreadLoop() {

    struct Stopped {
        ClientConnection& connection;
        ~Stopped() { connection.listenerStopped(); }
    } stopped{*this};

    try {

        LOG_DEBUG_LIB(LibFdb5) << "ClientConnection::listeningDataThreadLoop started" << std::endl;

        MessageHeader hdr;

        while (true) {

            Buffer payload = Connection::readData(hdr);

            LOG_DEBUG_LIB(LibFdb5) << "ClientConnection::listeningDataThreadLoop - " << msgHeader(hdr, dataEndpoint_)
                                   << std::endl;

            if (hdr.message == Message::Exit) {
                invalidate();
                awaitControlListener();
                // if the control listener has seen the server's error, the clients get that rather than a closure
                closeConnection(closureReason());
                return;
            }
            if (hdr.clientID()) {
                bool handled = false;
                ASSERT(!hdr.control());

                // The client cannot be removed while it handles the message (see withClient()). n.b. it may block
                // here on the full queue of a read: closeConnection() can still interrupt it.
                const bool found = withClient(hdr.clientID(), [&](Client& client) {
                    if (hdr.payloadSize == 0) {
                        handled = client.handle(hdr.message, hdr.requestID);
                    }
                    else {
                        handled = client.handle(hdr.message, hdr.requestID, std::move(payload));
                    }
                });
                if (!found && wasRemoved(hdr.clientID())) {
                    // the client went away while the server was still sending: nobody will read it
                    LOG_DEBUG_LIB(LibFdb5) << "Discarding " << msgHeader(hdr, dataEndpoint_) << " for a removed client"
                                           << std::endl;
                    continue;
                }
                if (!found) {
                    std::ostringstream ss;
                    ss << "ERROR: " << msgHeader(hdr, dataEndpoint_) << " - ClientID not found. ABORTING";
                    Log::status() << ss.str() << std::endl;
                    Log::error() << "Retrieving... " << ss.str() << std::endl;
                    throw SeriousBug(ss.str(), Here());
                }

                if (!handled) {
                    std::ostringstream ss;
                    ss << "ERROR: " << msgHeader(hdr, dataEndpoint_);
                    if (hdr.message == Message::Error) {
                        ss << " - received an unhandled error";
                        if (hdr.payloadSize) {
                            ss << ": " << errorText(payload);
                        }
                        throw RemoteFDBException(ss.str(), dataEndpoint_);
                    }
                    ss << " - received unexpected message. ABORTING";
                    Log::status() << ss.str() << std::endl;
                    Log::error() << "Client Retrieving... " << ss.str() << std::endl;
                    throw SeriousBug(ss.str(), Here());
                }
            }
            else if (hdr.message == Message::Error) {
                // not addressed to any client, so it cannot be delivered to a request
                throw RemoteFDBException("Received an error from server: " + errorText(payload), dataEndpoint_);
            }
        }

        // We don't want to let exceptions escape inside a worker thread.
    }
    catch (...) {
        awaitControlListener();
        handleConnectionError(std::current_exception());
    }
}

}  // namespace fdb5::remote
