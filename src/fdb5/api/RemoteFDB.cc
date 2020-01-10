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

#include <functional>
#include <unistd.h>

#include "fdb5/LibFdb5.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/api/RemoteFDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

#include "eckit/config/LocalConfiguration.h"
#include "eckit/io/Buffer.h"
#include "eckit/log/Bytes.h"
#include "eckit/log/Log.h"
#include "eckit/config/Resource.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/utils/Translator.h"
#include "eckit/runtime/Main.h"
#include "eckit/os/BackTrace.h"

#include "metkit/MarsRequest.h"

using namespace eckit;
using namespace eckit::net;
using namespace fdb5::remote;


namespace eckit {
template<> struct Translator<Endpoint, std::string> {
    std::string operator()(const net::Endpoint& e) {
        std::stringstream ss;
        ss << e;
        return ss.str();
    }
};
}

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

namespace {
class TCPException : public Exception {
public:
    TCPException(const std::string& msg, const CodeLocation& here) :
        Exception(std::string("TCPException: ") + msg, here) {

        eckit::Log::error() << "TCP Exception; backtrace(): " << std::endl;
        eckit::Log::error() << eckit::BackTrace::dump() << std::endl;
    }
};
}

class RemoteFDBException : public RemoteException {
public:
    RemoteFDBException(const std::string& msg, const net::Endpoint& endpoint):
        RemoteException(msg, eckit::Translator<Endpoint, std::string>()(endpoint)) {}
};

//----------------------------------------------------------------------------------------------------------------------

// n.b. if we get integer overflow, we reuse the IDs. This is not a
//      big deal. The idea that we could be on the 2.1 billionth (successful)
//      request, and still have an ongoing request 0 is ... laughable.

static uint32_t generateRequestID() {

    static std::mutex m;
    static uint32_t id = 0;

    std::lock_guard<std::mutex> lock(m);
    return ++id;
}


RemoteFDB::RemoteFDB(const eckit::Configuration& config, const std::string& name) :
    FDBBase(config, name),
    controlEndpoint_(config.getString("host"), config.getInt("port")),
    archiveID_(0),
    maxArchiveQueueLength_(eckit::Resource<size_t>("fdbRemoteArchiveQueueLength;$FDB_REMOTE_ARCHIVE_QUEUE_LENGTH", 200)),
    maxArchiveBatchSize_(config.getInt("maxBatchSize", 1)),
    retrieveMessageQueue_(eckit::Resource<size_t>("fdbRemoteRetrieveQueueLength;$FDB_REMOTE_RETRIEVE_QUEUE_LENGTH", 200)),
    connected_(false) {}


RemoteFDB::~RemoteFDB() {

    // If we have launched a thread with an async and we manage to get here, this is
    // an error. n.b. if we don't do something, we will block in the destructor
    // of std::future.

    if (archiveFuture_.valid()) {
        Log::error() << "Attempting to destruct RemoteFDB with active archive thread" << std::endl;
        eckit::Main::instance().terminate();
    }

    disconnect();
}


// Functions for management of the connection

// Protocol negotiation:
//
// i) Connect to server. Send:
//     - session identification
//     - supported functionality for protocol negotiation
//
// ii) Server responds. Sends:
//     - returns the client session id (for verification)
//     - the server session id
//     - endpoint for data connection
//     - selected functionality for protocol negotiation
//
// iii) Open data connection, and write to server:
//     - client session id
//     - server session id
//
// This appears quite verbose in terms of protocol negotiation, but the repeated
// sending of both session ids allows clients and servers to be sure that they are
// connected to the correct endpoint in a multi-process, multi-host environment.
//
// This was an issue on the NextGenIO prototype machine, where TCP connections were
// getting lost, and/or incorrectly reset, and as a result sessions were being
// mispaired by the network stack!

void RemoteFDB::connect() {

    if (!connected_) {

        // Connect to server, and check that the server is happy on the response

        Log::debug<LibFdb5>() << "Connecting to host: " << controlEndpoint_ << std::endl;
        controlClient_.connect(controlEndpoint_);
        writeControlStartupMessage();
        SessionID serverSession = verifyServerStartupResponse();

        // Connect to the specified data port

        Log::debug<LibFdb5>() << "Received data endpoint from host: " << dataEndpoint_ << std::endl;
        dataClient_.connect(dataEndpoint_);
        writeDataStartupMessage(serverSession);

        // And the connections are set up. Let everything start up!

        listeningThread_ = std::thread([this] { listeningThreadLoop(); });
        connected_ = true;
    }
}

void RemoteFDB::disconnect() {
    if (connected_) {

        // Send termination message
        controlWrite(Message::Exit, generateRequestID());

        listeningThread_.join();

        // Close both the control and data connections
        controlClient_.close();
        dataClient_.close();
        connected_ = false;
    }
}

void RemoteFDB::writeControlStartupMessage() {

    Buffer payload(4096);
    MemoryStream s(payload);
    s << sessionID_;
    s << controlEndpoint_;

    // TODO: Abstract this dictionary into a RemoteConfiguration object, which
    //       understands how to do the negotiation, etc, but uses Value (i.e.
    //       essentially JSON) over the wire for flexibility.
    s << availableFunctionality().get();

    controlWrite(Message::Startup, 0, payload.data(), s.position());
}

SessionID RemoteFDB::verifyServerStartupResponse() {

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
    LocalConfiguration serverFunctionality(Value(s));

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

void RemoteFDB::writeDataStartupMessage(const eckit::SessionID& serverSession) {

    Buffer payload(1024);
    MemoryStream s(payload);

    s << sessionID_;
    s << serverSession;

    dataWrite(Message::Startup, 0, payload.data(), s.position());
}

eckit::LocalConfiguration RemoteFDB::availableFunctionality() const {
    return LocalConfiguration();
}

void RemoteFDB::listeningThreadLoop() {

    /// @note This routine retrieves BOTH normal API asynchronously returned data, AND
    /// fields that are being returned by a retrieve() call. These need to go into different
    /// queues
    /// --> Test if the requestID is a known API request, otherwise push onto the retrieve queue


    /// @note messageQueues_ is a map of requestID:MessageQueue. At the point that
    /// a request is complete, errored or otherwise killed, it needs to be removed
    /// from the map. The shared_ptr allows this removal to be asynchronous with
    /// the actual task cleaning up and returning to the client.

    try {

    MessageHeader hdr;
    eckit::FixedString<4> tail;

    while (true) {

        dataRead(&hdr, sizeof(hdr));

        ASSERT(hdr.marker == StartMarker);
        ASSERT(hdr.version == CurrentVersion);

        switch (hdr.message) {

        case Message::Exit:
            return;

        case Message::Blob: {
            Buffer payload(hdr.payloadSize);
            if (hdr.payloadSize > 0) dataRead(payload, hdr.payloadSize);

            auto it = messageQueues_.find(hdr.requestID);
            if (it != messageQueues_.end()) {
                it->second->emplace(std::make_pair(hdr, std::move(payload)));
            } else {
                retrieveMessageQueue_.emplace(std::make_pair(hdr, std::move(payload)));
            }
            break;
        }

        case Message::Complete: {
            auto it = messageQueues_.find(hdr.requestID);
            if (it != messageQueues_.end()) {
                it->second->close();

                // Remove entry (shared_ptr --> message queue will be destroyed when it
                // goes out of scope in the worker thread).
                messageQueues_.erase(it);

            } else {
                retrieveMessageQueue_.emplace(std::make_pair(hdr, Buffer(0)));
            }
            break;
        }

        case Message::Error: {

            auto it = messageQueues_.find(hdr.requestID);
            if (it != messageQueues_.end()) {
                std::string msg;
                if (hdr.payloadSize > 0) {
                    msg.resize(hdr.payloadSize, ' ');
                    dataRead(&msg[0], hdr.payloadSize);
                }
                it->second->interrupt(std::make_exception_ptr(RemoteFDBException(msg, dataEndpoint_)));

                // Remove entry (shared_ptr --> message queue will be destroyed when it
                // goes out of scope in the worker thread).
                messageQueues_.erase(it);

            } else if (hdr.requestID == archiveID_) {

                std::lock_guard<std::mutex> lock(archiveQueuePtrMutex_);

                if (archiveQueue_) {
                    std::string msg;
                    if (hdr.payloadSize > 0) {
                        msg.resize(hdr.payloadSize, ' ');
                        dataRead(&msg[0], hdr.payloadSize);
                    }

                    archiveQueue_->interrupt(std::make_exception_ptr(RemoteFDBException(msg, dataEndpoint_)));
                }
            } else {
                Buffer payload(hdr.payloadSize);
                if (hdr.payloadSize > 0) dataRead(payload, hdr.payloadSize);
                retrieveMessageQueue_.emplace(std::make_pair(hdr, std::move(payload)));
            }
            break;
        }

        default: {
            std::stringstream ss;
            ss << "ERROR: Unexpected message recieved (" << static_cast<int>(hdr.message) << "). ABORTING";
            Log::status() << ss.str() << std::endl;
            Log::error() << "Retrieving... " << ss.str() << std::endl;
            throw SeriousBug(ss.str(), Here());
        }
        };

        // Ensure we have consumed exactly the correct amount from the socket.

        dataRead(&tail, sizeof(tail));
        ASSERT(tail == EndMarker);
    }

    // We don't want to let exceptions escape inside a worker thread.

    } catch (const std::exception& e) {
        for (auto& it : messageQueues_) {
            it.second->interrupt(std::make_exception_ptr(e));
        }
        messageQueues_.clear();
        retrieveMessageQueue_.interrupt(std::make_exception_ptr(e));
        {
            std::lock_guard<std::mutex> lock(archiveQueuePtrMutex_);
            if (archiveQueue_) archiveQueue_->interrupt(std::make_exception_ptr(e));
        }
    } catch (...) {
        for (auto& it : messageQueues_) {
            it.second->interrupt(std::current_exception());
        }
        messageQueues_.clear();
        retrieveMessageQueue_.interrupt(std::current_exception());
        {
            std::lock_guard<std::mutex> lock(archiveQueuePtrMutex_);
            if (archiveQueue_) archiveQueue_->interrupt(std::current_exception());
        }
    }
}

void RemoteFDB::controlWriteCheckResponse(Message msg, uint32_t requestID, const void* payload, uint32_t payloadLength) {

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

void RemoteFDB::controlWrite(Message msg, uint32_t requestID, const void* payload, uint32_t payloadLength) {

    ASSERT((payload == nullptr) == (payloadLength == 0));

    MessageHeader message(msg, requestID, payloadLength);
    controlWrite(&message, sizeof(message));
    if (payload) {
        controlWrite(payload, payloadLength);
    }
    controlWrite(&EndMarker, sizeof(EndMarker));
}

void RemoteFDB::controlWrite(const void* data, size_t length) {
    size_t written = controlClient_.write(data, length);
    if (length != written) {
        std::stringstream ss;
        ss << "Write error. Expected " << length << " bytes, wrote " << written;
        throw TCPException(ss.str(), Here());
    }
}

void RemoteFDB::controlRead(void* data, size_t length) {
    size_t read = controlClient_.read(data, length);
    if (length != read) {
        std::stringstream ss;
        ss << "Read error. Expected " << length << " bytes, read " << read;
        throw TCPException(ss.str(), Here());
    }
}

void RemoteFDB::dataWrite(Message msg, uint32_t requestID, const void* payload, uint32_t payloadLength) {

    ASSERT((payload == nullptr) == (payloadLength == 0));

    MessageHeader message(msg, requestID, payloadLength);
    dataWrite(&message, sizeof(message));
    if (payload) {
        dataWrite(payload, payloadLength);
    }
    dataWrite(&EndMarker, sizeof(EndMarker));
}

void RemoteFDB::dataWrite(const void* data, size_t length) {
    size_t written = dataClient_.write(data, length);
    if (length != written) {
        std::stringstream ss;
        ss << "Write error. Expected " << length << " bytes, wrote " << written;
        throw TCPException(ss.str(), Here());
    }
}

void RemoteFDB::dataRead(void* data, size_t length) {
    size_t read = dataClient_.read(data, length);
    if (length != read) {
        std::stringstream ss;
        ss << "Read error. Expected " << length << " bytes, read " << read;
        throw TCPException(ss.str(), Here());
    }
}

void RemoteFDB::handleError(const MessageHeader& hdr) {

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

        throw RemoteFDBException(what, controlEndpoint_);
    }
}

FDBStats RemoteFDB::stats() const {
    return internalStats_;
}


// Implement the primary FDB API

// -----------------------------------------------------------------------------------------------------
// Helper classes describe the behaviour of the various API functions to be forwarded
// -----------------------------------------------------------------------------------------------------

namespace {

template <typename T, Message msgID>
struct BaseAPIHelper {

    typedef T ValueType;

    static size_t bufferSize() { return 4096; }
    static size_t queueSize() { return 100; }
    static Message message() { return msgID; }

    void encodeExtra(eckit::Stream& s) const {}
    static ValueType valueFromStream(eckit::Stream& s) { return ValueType(s); }
};

using ListHelper = BaseAPIHelper<ListElement, Message::List>;

using StatsHelper = BaseAPIHelper<StatsElement, Message::Stats>;

using StatusHelper = BaseAPIHelper<StatusElement, Message::Status>;

struct DumpHelper : BaseAPIHelper<DumpElement, Message::Dump> {

    DumpHelper(bool simple) : simple_(simple) {}
    void encodeExtra(eckit::Stream& s) const { s << simple_; }
    static DumpElement valueFromStream(eckit::Stream& s) {
        DumpElement elem;
        s >> elem;
        return elem;
    }

private:
    bool simple_;
};

struct PurgeHelper : BaseAPIHelper<PurgeElement, Message::Purge> {

    PurgeHelper(bool doit, bool porcelain) : doit_(doit), porcelain_(porcelain) {}
    void encodeExtra(eckit::Stream& s) const {
        s << doit_;
        s << porcelain_;
    }
    static PurgeElement valueFromStream(eckit::Stream& s) {
        PurgeElement elem;
        s >> elem;
        return elem;
    }

private:
    bool doit_;
    bool porcelain_;
};

struct WipeHelper : BaseAPIHelper<WipeElement, Message::Wipe> {

    WipeHelper(bool doit, bool porcelain, bool unsafeWipeAll) :
        doit_(doit), porcelain_(porcelain), unsafeWipeAll_(unsafeWipeAll) {}
    void encodeExtra(eckit::Stream& s) const {
        s << doit_;
        s << porcelain_;
        s << unsafeWipeAll_;
    }
    static WipeElement valueFromStream(eckit::Stream& s) {
        WipeElement elem;
        s >> elem;
        return elem;
    }

private:
    bool doit_;
    bool porcelain_;
    bool unsafeWipeAll_;
};

struct ControlHelper : BaseAPIHelper<ControlElement, Message::Control> {

    ControlHelper(ControlAction action, ControlIdentifiers identifiers) :
        action_(action),
        identifiers_(identifiers) {}
    void encodeExtra(eckit::Stream& s) const {
        s << action_;
        s << identifiers_;
    }

private:
    ControlAction action_;
    ControlIdentifiers identifiers_;
};

} // namespace

// -----------------------------------------------------------------------------------------------------

// forwardApiCall captures the asynchronous behaviour:
//
// i) Set up a Queue to receive the messages as they come in
// ii) Encode the request+arguments and send them to the server
// iii) Return an AsyncIterator that pulls messages off the queue, and returns them to the caller.


template <typename HelperClass>
auto RemoteFDB::forwardApiCall(const HelperClass& helper, const FDBToolRequest& request) -> APIIterator<typename HelperClass::ValueType> {

    using ValueType = typename HelperClass::ValueType;
    using IteratorType = APIIterator<ValueType>;
    using AsyncIterator = APIAsyncIterator<ValueType>;

    connect();

    // Ensure we have an entry in the message queue before we trigger anything that
    // will result in return messages

    uint32_t id = generateRequestID();
    auto entry = messageQueues_.emplace(id, std::make_shared<MessageQueue>(HelperClass::queueSize()));
    ASSERT(entry.second);
    std::shared_ptr<MessageQueue> messageQueue(entry.first->second);

    // Encode the request and send it to the server

    Buffer encodeBuffer(HelperClass::bufferSize());
    MemoryStream s(encodeBuffer);
    s << request;
    helper.encodeExtra(s);

    controlWriteCheckResponse(HelperClass::message(), id, encodeBuffer, s.position());

    // Return an AsyncIterator to allow the messages to be retrieved in the API

    return IteratorType(
                // n.b. Don't worry about catching exceptions in lambda, as
                // this is handled in the AsyncIterator.
                new AsyncIterator (
                    [messageQueue](eckit::Queue<ValueType>& queue) {
                        StoredMessage msg = std::make_pair(remote::MessageHeader{}, 0);
                        while (true) {
                            if (messageQueue->pop(msg) == -1) {
                                break;
                            } else {
                                MemoryStream s(msg.second);
                                queue.emplace(HelperClass::valueFromStream(s));
                            }
                        }
                        // messageQueue goes out of scope --> destructed
                    }
                )
           );
}

ListIterator RemoteFDB::list(const FDBToolRequest& request) {
    return forwardApiCall(ListHelper(), request);
}


DumpIterator RemoteFDB::dump(const FDBToolRequest& request, bool simple) {
    return forwardApiCall(DumpHelper(simple), request);
}

StatusIterator RemoteFDB::status(const FDBToolRequest& request) {
    return forwardApiCall(StatusHelper(), request);
}

WipeIterator RemoteFDB::wipe(const FDBToolRequest& request, bool doit, bool porcelain, bool unsafeWipeAll) {
    return forwardApiCall(WipeHelper(doit, porcelain, unsafeWipeAll), request);
}

PurgeIterator RemoteFDB::purge(const FDBToolRequest& request, bool doit, bool porcelain) {
    return forwardApiCall(PurgeHelper(doit, porcelain), request);
}

StatsIterator RemoteFDB::stats(const FDBToolRequest& request) {
    return forwardApiCall(StatsHelper(), request);
}

ControlIterator RemoteFDB::control(const FDBToolRequest& request,
                                   ControlAction action,
                                   ControlIdentifiers identifiers) {
    return forwardApiCall(ControlHelper(action, identifiers), request);
};

// -----------------------------------------------------------------------------------------------------

// Here we do archive/flush related stuff

void RemoteFDB::archive(const Key& key, const void* data, size_t length) {

    connect();

    // if there is no archiving thread active, then start one.
    // n.b. reset the archiveQueue_ after a potential flush() cycle.

    if (!archiveFuture_.valid()) {

        // Start the archival request on the remote side
        ASSERT(archiveID_ == 0);
        uint32_t id = generateRequestID();
        controlWriteCheckResponse(Message::Archive, id);
        archiveID_ = id;

        // Reset the queue after previous done/errors
        {
            std::lock_guard<std::mutex> lock(archiveQueuePtrMutex_);
            ASSERT(!archiveQueue_);
            archiveQueue_.reset(new ArchiveQueue(maxArchiveQueueLength_));
        }

        archiveFuture_ = std::async(std::launch::async, [this, id] { return archiveThreadLoop(id); });
    }

    ASSERT(archiveFuture_.valid());
    ASSERT(archiveID_ != 0);
    {
        std::lock_guard<std::mutex> lock(archiveQueuePtrMutex_);
        ASSERT(archiveQueue_);
        archiveQueue_->emplace(std::make_pair(key, Buffer(reinterpret_cast<const char*>(data), length)));
    }
}


void RemoteFDB::flush() {

    Timer timer;

    timer.start();

    // Flush only does anything if there is an ongoing archive();
    if (archiveFuture_.valid()) {

        ASSERT(archiveID_ != 0);
        {
            ASSERT(archiveQueue_);
            std::lock_guard<std::mutex> lock(archiveQueuePtrMutex_);
            archiveQueue_->close();
        }
        FDBStats stats = archiveFuture_.get();
        ASSERT(!archiveQueue_);
        archiveID_ = 0;

        ASSERT(stats.numFlush() == 0);
        size_t numArchive = stats.numArchive();

        Buffer sendBuf(4096);
        MemoryStream s(sendBuf);
        s << numArchive;

        // The flush call is blocking
        controlWriteCheckResponse(Message::Flush, generateRequestID(), sendBuf, s.position());

        internalStats_ += stats;
    }

    timer.stop();
    internalStats_.addFlush(timer);
}


FDBStats RemoteFDB::archiveThreadLoop(uint32_t requestID) {

    FDBStats localStats;
    eckit::Timer timer;

    // We can pop multiple elements off the archive queue simultaneously, if
    // configured
    std::vector<std::pair<Key, Buffer>> elements;
    for (size_t i = 0; i < maxArchiveBatchSize_; ++i) {
        elements.emplace_back(std::make_pair(Key{}, Buffer{0}));
    }

    try {

        ASSERT(archiveQueue_);
        ASSERT(archiveID_ != 0);

        long popped;
        while ((popped = archiveQueue_->pop(elements)) != -1) {

            timer.start();
            long dataSent = sendArchiveData(requestID, elements, popped);
            timer.stop();
            localStats.addArchive(dataSent, timer, popped);
        }

        // And note that we are done. (don't time this, as already being blocked
        // on by the ::flush() routine)

        MessageHeader hdr(Message::Flush, requestID);
        dataWrite(&hdr, sizeof(hdr));
        dataWrite(&EndMarker, sizeof(EndMarker));

        archiveID_ = 0;
        archiveQueue_.reset();

    } catch (...) {
        archiveQueue_->interrupt(std::current_exception());
        throw;
    }

    return localStats;

    // We are inside an async, so don't need to worry about exceptions escaping.
    // They will be released when flush() is called.
}

long RemoteFDB::sendArchiveData(uint32_t id, const std::vector<std::pair<Key, Buffer>>& elements, size_t count) {

    if (count == 1) {
        sendArchiveData(id, elements[0].first, elements[0].second.data(), elements[0].second.size());
        return elements[0].second.size();
    }

    // Serialise the keys

    std::vector<Buffer> keyBuffers;
    std::vector<size_t> keySizes;
    keyBuffers.reserve(count);
    keySizes.reserve(count);

    size_t containedSize = 0;

    for (size_t i = 0; i < count; ++i) {
        keyBuffers.emplace_back(Buffer {4096});
        MemoryStream keyStream(keyBuffers.back());
        keyStream << elements[i].first;
        keySizes.push_back(keyStream.position());
        containedSize += (keyStream.position() + elements[i].second.size() +
                          sizeof(MessageHeader) + sizeof(EndMarker));
    }

    // Construct the containing message

    MessageHeader message(Message::MultiBlob, id, containedSize);
    dataWrite(&message, sizeof(message));

    long dataSent = 0;

    for (size_t i = 0; i < count; ++i) {
        MessageHeader containedMessage(Message::Blob, id, elements[i].second.size() + keySizes[i]);
        dataWrite(&containedMessage, sizeof(containedMessage));
        dataWrite(keyBuffers[i], keySizes[i]);
        dataWrite(elements[i].second.data(), elements[i].second.size());
        dataWrite(&EndMarker, sizeof(EndMarker));
        dataSent += elements[i].second.size();
    }

    dataWrite(&EndMarker, sizeof(EndMarker));
    return dataSent;
}


void RemoteFDB::sendArchiveData(uint32_t id, const Key& key, const void* data, size_t length) {

    ASSERT(data);
    ASSERT(length != 0);

    Buffer keyBuffer(4096);
    MemoryStream keyStream(keyBuffer);
    keyStream << key;

    MessageHeader message(Message::Blob, id, length + keyStream.position());
    dataWrite(&message, sizeof(message));
    dataWrite(keyBuffer, keyStream.position());
    dataWrite(data, length);
    dataWrite(&EndMarker, sizeof(EndMarker));
}

// -----------------------------------------------------------------------------------------------------

//
/// @note The DataHandles returned by retrieve() MUST STRICTLY be read in order.
///       We do not create multiple message queues, one for each requestID, even
///       though that would be nice. This is because a commonly used retrieve
///       pattern uses many retrieve() calls aggregated into a MultiHandle, and
///       if we created lots of queues we would just run out of memory receiving
///       from the stream. Further, if we curcumvented this by blocking, then we
///       could get deadlocked if we try and read a message that is further back
///       in the stream
///
/// --> Retrieve is a _streaming_ service.

namespace {

class FDBRemoteDataHandle : public DataHandle {

public: // methods

    FDBRemoteDataHandle(uint32_t requestID,
                        RemoteFDB::MessageQueue& queue,
                        const net::Endpoint& remoteEndpoint) :
        requestID_(requestID),
        queue_(queue),
        remoteEndpoint_(remoteEndpoint),
        pos_(0),
        overallPosition_(0),
        currentBuffer_(0),
        complete_(false) {}

private: // methods

    void print(std::ostream& s) const override {
        s << "FDBRemoteDataHandle(id=" << requestID_ << ")";
    }

    Length openForRead() override {
        return estimate();
    }
    void openForWrite(const Length&) override { NOTIMP; }
    void openForAppend(const Length&) override { NOTIMP; }
    long write(const void*, long) override { NOTIMP; }
    void close() override {}

    long read(void* pos, long sz) override {

        if (complete_) return 0;

        if (currentBuffer_.size() != 0) return bufferRead(pos, sz);

        // If we are in the DataHandle, then there MUST be data to read

        RemoteFDB::StoredMessage msg = std::make_pair(remote::MessageHeader{}, 0);
        ASSERT(queue_.pop(msg) != -1);

        // TODO; Error handling in the retrieve pathway

        MessageHeader& hdr(msg.first);

        ASSERT(hdr.marker == StartMarker);
        ASSERT(hdr.version == CurrentVersion);

        // Handle any remote errors communicated from the server

        if (hdr.message == Message::Error) {
            std::string errmsg(static_cast<const char*>(msg.second), msg.second.size());
            throw RemoteFDBException(errmsg, remoteEndpoint_);
        }

        // Are we now complete

        if (hdr.message == Message::Complete) {
            complete_ = 0;
            return 0;
        }

        ASSERT(hdr.message == Message::Blob);

        // Otherwise return the data!

        std::swap(currentBuffer_, msg.second);

        return bufferRead(pos, sz);
    }

    // A helper function that returns some, or all, of a buffer that has
    // already been retrieved.

    long bufferRead(void* pos, long sz) {

        ASSERT(currentBuffer_.size() != 0);
        ASSERT(pos_ < currentBuffer_.size());

        long read = std::min(sz, long(currentBuffer_.size() - pos_));

        ::memcpy(pos, &currentBuffer_[pos_], read);
        pos_ += read;
        overallPosition_ += read;

        // If we have exhausted this buffer, free it up.

        if (pos_ >= currentBuffer_.size()) {
            Buffer nullBuffer(0);
            std::swap(currentBuffer_, nullBuffer);
            pos_ = 0;
            ASSERT(currentBuffer_.size() == 0);
        }

        return read;
    }

    Length estimate() override {
        return 0;
    }

    Offset position() override {
        return overallPosition_;
    }

private: // members

    uint32_t requestID_;
    RemoteFDB::MessageQueue& queue_;
    net::Endpoint remoteEndpoint_;
    size_t pos_;
    Offset overallPosition_;
    Buffer currentBuffer_;
    bool complete_;
};

}

// Here we do (asynchronous) retrieving related stuff

DataHandle* RemoteFDB::retrieve(const metkit::MarsRequest& request) {

    connect();

    Buffer encodeBuffer(4096);
    MemoryStream s(encodeBuffer);
    s << request;

    uint32_t id = generateRequestID();

    controlWriteCheckResponse(Message::Retrieve, id, encodeBuffer, s.position());

    return new FDBRemoteDataHandle(id, retrieveMessageQueue_, controlEndpoint_);
}


void RemoteFDB::print(std::ostream &s) const {
    s << "RemoteFDB(host=" << controlEndpoint_ << ", data=" << dataEndpoint_ << ")";
}

static FDBBuilder<RemoteFDB> remoteFdbBuilder("remote");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
