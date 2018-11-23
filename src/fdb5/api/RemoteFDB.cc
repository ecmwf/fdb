/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <functional>

#include "fdb5/LibFdb.h"
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

#include "marslib/MarsRequest.h"

// TODO: Write things as one chunk?

using namespace eckit;
using namespace fdb5::remote;


namespace fdb5 {

static FDBBuilder<RemoteFDB> remoteFdbBuilder("remote");

//----------------------------------------------------------------------------------------------------------------------

class TCPException : public Exception {
public:
    TCPException(const std::string& msg, const CodeLocation& here) :
        Exception(std::string("TCPException: ") + msg, here) {}
};

//----------------------------------------------------------------------------------------------------------------------

// n.b. if we get integer overflow, we reuse the IDs. This is not a
//      big deal. The idea that we could be on the 2.1 billionth (successful)
//      request, and still have an ongoing request 0 is ... laughable.

static uint32_t generateRequestID() {

    static std::mutex m;
    static uint32_t id = 0;

    std::lock_guard<std::mutex> lock(m);
    return id++;
}


RemoteFDB::RemoteFDB(const eckit::Configuration& config, const std::string& name) :
    FDBBase(config, name),
    hostname_(config.getString("host")),
    port_(config.getLong("port")),
    dataport_(0),
    connected_(false) {}


RemoteFDB::~RemoteFDB() {
    disconnect();
}

// Functions for management of the connection

void RemoteFDB::connect() {

    if (!connected_) {
        controlClient_.connect(hostname_, port_);

        // Get data connection port, and connect to it too
        controlRead(&dataport_, sizeof(dataport_));
        Log::debug<LibFdb>() << "Recieved data port from host: " << hostname_ << ":" << dataport_ << std::endl;
        dataClient_.connect(hostname_, dataport_);

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

void RemoteFDB::listeningThreadLoop() {

    MessageHeader hdr;
    eckit::FixedString<4> tail;

//    Log::info() << "In listening thread loop" << std::endl;

    while (true) {

        dataRead(&hdr, sizeof(hdr));

        ASSERT(hdr.marker == StartMarker);
        ASSERT(hdr.version == CurrentVersion);
//        Log::info() << "(data) Got header id: " << hdr.requestID << std::endl;

        switch (hdr.message) {

        case Message::Exit:
//            Log::info() << "Exit message received from server" << std::endl;
            return;

        case Message::Blob: {
//            Log::info() << "Received data blob from server" << std::endl;
            Buffer payload(hdr.payloadSize);
            if (hdr.payloadSize > 0) dataRead(payload, hdr.payloadSize);

            auto it = messageQueues_.find(hdr.requestID);
            ASSERT(it != messageQueues_.end());
            it->second.emplace(std::make_pair(hdr, std::move(payload)));
            break;
        }

        case Message::Complete: {
//            Log::info() << "Complete message received from server" << std::endl;
            auto it = messageQueues_.find(hdr.requestID);
            ASSERT(it != messageQueues_.end());
            it->second.set_done();
            break;
        }

        case Message::Error: {
            auto it = messageQueues_.find(hdr.requestID);
            ASSERT(it != messageQueues_.end());
            std::string msg;
            if (hdr.payloadSize > 0) {
                msg.resize(hdr.payloadSize, ' ');
                dataRead(&msg[0], hdr.payloadSize);
            }
            std::stringstream ss;
            ss << "[" << controlClient_.remoteHost() << ":" << controlClient_.remotePort() << "]: ";
            it->second.interrupt(ss.str() + msg);
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

//        eckit::Log::info() << "Reading tail" << std::endl;
        dataRead(&tail, sizeof(tail));
        ASSERT(tail == EndMarker);
    }
}

void RemoteFDB::controlWrite(Message msg, uint32_t requestID, void* payload, uint32_t payloadLength) {

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

void RemoteFDB::dataRead(void *data, size_t length) {
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

        throw RemoteException(what, hostname_);
    }
}



// Implement the primary FDB API

void RemoteFDB::archive(const Key& key, const void* data, size_t length) { NOTIMP; }

DataHandle* RemoteFDB::retrieve(const MarsRequest& request) { NOTIMP; }


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

//using WhereHelper = BaseAPIHelper<WhereElement>;

using StatsHelper = BaseAPIHelper<StatsElement, Message::Stats>;

struct DumpHelper : BaseAPIHelper<DumpElement, Message::Dump> {

    typedef DumpElement ValueType;

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

    typedef PurgeElement ValueType;

    PurgeHelper(bool doit) : doit_(doit) {}
    void encodeExtra(eckit::Stream& s) const { s << doit_; }
    static PurgeElement valueFromStream(eckit::Stream& s) {
        PurgeElement elem;
        s >> elem;
        return elem;
    }

private:
    bool doit_;
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
    auto entry = messageQueues_.emplace(id, HelperClass::queueSize());
    ASSERT(entry.second);
    MessageQueue& messageQueue(entry.first->second);

    // Encode the request and send it to the server

    Buffer encodeBuffer(HelperClass::bufferSize());
    MemoryStream s(encodeBuffer);
    s << request;
    helper.encodeExtra(s);

    controlWrite(HelperClass::message(), id, encodeBuffer, s.position());

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

    // Return an AsyncIterator to allow the messages to be retrieved in the API

    return IteratorType(
                // n.b. Don't worry about catching exceptions in lambda, as
                // this is handled in the AsyncIterator.
                new AsyncIterator (
                    [&messageQueue](eckit::Queue<ValueType>& queue) {
                        StoredMessage msg(std::make_pair(MessageHeader(), Buffer(0)));
                        while (true) {
                            if (messageQueue.pop(msg) == -1) {
                                break;
                            } else {
                                MemoryStream s(msg.second);
                                queue.emplace(HelperClass::valueFromStream(s));
                            }
                        }
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

WhereIterator RemoteFDB::where(const FDBToolRequest& request) {
    NOTIMP;
    //return forwardApiCall(WhereHelper(), request);
}

WipeIterator RemoteFDB::wipe(const FDBToolRequest& request, bool doit) { NOTIMP; }

PurgeIterator RemoteFDB::purge(const FDBToolRequest& request, bool doit) {
    return forwardApiCall(PurgeHelper(doit), request);
}

StatsIterator RemoteFDB::stats(const FDBToolRequest& request) {
    return forwardApiCall(StatsHelper(), request);
}

void RemoteFDB::flush() { NOTIMP; }



#if 0
RemoteFDB::RemoteFDB(const eckit::Configuration& config, const std::string& name) :
    FDBBase(config, name),
    hostname_(config.getString("host")),
    port_(config.getLong("port")),
//    archivePosition_(0),
    archiveQueue_(eckit::Resource<size_t>("queue-length", 200)),
    connected_(false),
    noThreadArchive_(eckit::Resource<bool>("fdbRemoteNoThreadArchive;$FDB_REMOTE_NO_THREAD_ARCHIVE", false)) {

    ASSERT(config.getString("type", "") == "remote");
}

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


void RemoteFDB::connect() {
    if (!connected_) {
        client_.connect(hostname_, port_);
//        stream_.reset(new TCPStream(client_.connect(hostname_, port_)));
        connected_ = true;
    }
}


void RemoteFDB::disconnect() {
    if (connected_) {
        MessageHeader msg(Message::Exit);
        clientWrite(&msg, sizeof(msg));
        clientWrite(&EndMarker, sizeof(EndMarker));
        client_.close();
//        msg.encode(*stream_);
//        stream_.reset();
        connected_ = false;
    }
}



void RemoteFDB::archive(const Key& key, const void* data, size_t length) {

    if (noThreadArchive_) {

        timer_.start();
        addToArchiveBuffer(key, data, length);
        timer_.stop();
        internalStats_.addArchive(length, timer_);

    } else {

        // If we aren't running a worker task to shift the data, then do that.
        if (!archiveFuture_.valid()) {
            archiveFuture_ = std::async(std::launch::async, [this] { return archiveThreadLoop(); });
        }

        // n.b. copies the key, but moves the new buffer.
        archiveQueue_.emplace(std::make_pair(key, Buffer(reinterpret_cast<const char*>(data), length)));
    }
}


FDBStats RemoteFDB::archiveThreadLoop() {

    FDBStats localStats;
    eckit::Timer timer;

    std::pair<Key, Buffer> element {Key{}, 0};

    while (true) {

        archiveQueue_.pop(element);

        const Key& key(element.first);
        const Buffer& buffer(element.second);

        if (buffer.size() == 0) {
            ASSERT(key.empty());
            timer.start();
            doBlockingFlush();
            timer.stop();
            localStats.addFlush(timer);
            return localStats;
        }

        timer.start();
        addToArchiveBuffer(key, buffer, buffer.size());
        timer.stop();
        localStats.addArchive(buffer.size(), timer);
    }
}


#if 0
void RemoteFDB::doBlockingArchive(const Key& key, const eckit::Buffer& buffer) {

    connect();

    Buffer keyBuffer(4096);
    MemoryStream keyStream(keyBuffer);
    keyStream << key;

    MessageHeader msg(Message::Archive, buffer.size() + keyStream.position());

    clientWrite(&msg, sizeof(msg));
    clientWrite(keyBuffer, keyStream.position());
    clientWrite(buffer, buffer.size());
    clientWrite(&EndMarker, sizeof(EndMarker));
}
#endif


void RemoteFDB::addToArchiveBuffer(const Key& key, const void* data, size_t length) {

    static size_t bufferSize = eckit::Resource<size_t>("fdbRemoteSendBufferSize;$FDB_REMOTE_SEND_BUFFER_SIZE", 64 * 1024 * 1024);

    if (!archiveBuffer_) {
        archiveBuffer_.reset(new Buffer(bufferSize));
        archivePosition_ = 0;
    }

    Buffer keyBuffer(4096);
    MemoryStream keyStream(keyBuffer);
    keyStream << key;

    if (archivePosition_ + sizeof(MessageHeader) + keyStream.position() + length + sizeof(EndMarker) > bufferSize) {
        sendArchiveBuffer();
    }

    ASSERT(archivePosition_ + sizeof(MessageHeader) + keyStream.position() + length + sizeof(EndMarker) <= bufferSize);

    // Construct message header in place

    new (&(*archiveBuffer_)[archivePosition_]) MessageHeader(Message::Archive, length + keyStream.position());
    archivePosition_ += sizeof(MessageHeader);

    // Add the key to the stream

    ::memcpy(&(*archiveBuffer_)[archivePosition_], keyBuffer, keyStream.position());
    archivePosition_ += keyStream.position();

    // And the data

    ::memcpy(&(*archiveBuffer_)[archivePosition_], data, length);
    archivePosition_ += length;

    // And we are done

    ::memcpy(&(*archiveBuffer_)[archivePosition_], &EndMarker, sizeof(EndMarker));
    archivePosition_ += sizeof(EndMarker);
}

/// A data handle that pulls the specified amount of data from a socket into a buffer,
/// and then stores it there until requested.

class RetrieveDataHandle : public DataHandle {

public: // methods

    RetrieveDataHandle(size_t length) :
        length_(length),
        pos_(0),
        buffer_(length),
        read_(false) {}

    /// This is only not in the constructor to avoid the risk of exceptions being thrown
    /// in the constructor. Not well supported until std::make_unique in c++14
    void doRetrieve(const std::function<void(void*, size_t)>& readfn) {
        readfn(buffer_, length_);
        read_ = true;
    }

    virtual void print(std::ostream& s) const { s << "RetrieveDataHandle(size=" << Bytes(length_) << ")";}

    virtual Length openForRead() { return length_; }
    virtual void close() {}

    virtual long read(void* pos, long sz) {

        ASSERT(read_);
        ASSERT(length_ >= pos_);

        if (sz >= (length_ - pos_)) {
            sz = (length_ - pos_);
        }

        if (sz > 0) {
            ::memcpy(pos, &buffer_[pos_], sz);
            pos_ += sz;
        }

        ASSERT(length_ >= pos_);
        return sz;
    }


    virtual void openForWrite(const Length&) { NOTIMP; }
    virtual void openForAppend(const Length&) { NOTIMP; }

    virtual long write(const void*,long) { NOTIMP; }

private: // methods

    size_t pos_;
    size_t length_;
    Buffer buffer_;
    bool read_;
};


eckit::DataHandle* RemoteFDB::retrieve(const MarsRequest& request) {

    eckit::Log::info() << "Retrieving ..." << std::endl;

    connect();

    Buffer requestBuffer(4096);
    MemoryStream requestStream(requestBuffer);
    request.encode(requestStream);

    MessageHeader msg(Message::Retrieve, requestStream.position());

    clientWrite(&msg, sizeof(msg));
    clientWrite(requestBuffer, requestStream.position());
    clientWrite(&EndMarker, sizeof(EndMarker));

    MessageHeader response;
    eckit::FixedString<4> tail;

    clientRead(&response, sizeof(MessageHeader));

    handleError(response);

    ASSERT(response.marker == StartMarker);
    ASSERT(response.version == CurrentVersion);
    ASSERT(response.message == Message::Blob);

    eckit::ScopedPtr<RetrieveDataHandle> dh(new RetrieveDataHandle(response.payloadSize));
    dh->doRetrieve([this](void* data, size_t len) { this->clientRead(data, len); });

    clientRead(&tail, 4);
    ASSERT(tail == EndMarker);

    eckit::Log::info() << "Retrieved " << response.payloadSize << " bytes!!!!" << std::endl;

    return dh.release();
}


FDBListObject fdb5::RemoteFDB::list(const fdb5::FDBToolRequest &request) {
    NOTIMP;
}


std::string RemoteFDB::id() const {
    eckit::Translator<size_t, std::string> toStr;
    return std::string("remote=") + hostname_ + ":" + toStr(port_);
}


void RemoteFDB::flush() {

    Log::info() << "Flushing ..." << std::endl;

    if (noThreadArchive_) {

        timer_.start();
        doBlockingFlush();
        timer_.stop();
        internalStats_.addFlush(timer_);

    } else {

        // If we have a worker thread running (i.e. data has been written), then signal
        // a flush to it, and wait for it to be done.
        if (archiveFuture_.valid()) {
            archiveQueue_.emplace(std::pair<Key, Buffer>{Key{}, 0});
            internalStats_ += archiveFuture_.get();
        }
    }
}

FDBStats RemoteFDB::stats() const {
    return internalStats_;
}


void RemoteFDB::sendArchiveBuffer() {

    ASSERT(archivePosition_ > 0);
    ASSERT(archiveBuffer_);

    connect();

    Log::info() << "Sending archive buffer" << std::endl;

    clientWrite(*archiveBuffer_, archivePosition_);
    archivePosition_ = 0;

    // n.b. we do not need a response. If TCP is happy that it has gone, then it has
    //      gone. We handle errors from the other end in flush();
}


void RemoteFDB::doBlockingFlush() {

    /// If we have never written any data (i.e. not dirty) then we should never have
    /// got here, so this should be allocated
    ASSERT(archiveBuffer_);

    if (archivePosition_ + sizeof(MessageHeader) + sizeof(EndMarker) > archiveBuffer_->size()) {
        sendArchiveBuffer();
    }

    // Construct the flush message

    ASSERT(archivePosition_ + sizeof(MessageHeader) + sizeof(EndMarker) <= archiveBuffer_->size());

    new (&(*archiveBuffer_)[archivePosition_]) MessageHeader(Message::Flush);
    archivePosition_ += sizeof(MessageHeader);

    ::memcpy(&(*archiveBuffer_)[archivePosition_], &EndMarker, sizeof(EndMarker));
    archivePosition_ += sizeof(EndMarker);

    // Push all the data through, including any unsent fields

    sendArchiveBuffer();

    // Wait for the response

    MessageHeader response;
    eckit::FixedString<4> tail;

    clientRead(&response, sizeof(MessageHeader));

    handleError(response);

    ASSERT(response.marker == StartMarker);
    ASSERT(response.version == CurrentVersion);
    ASSERT(response.message == Message::Complete);

    clientRead(&tail, sizeof(tail));
    ASSERT(tail == EndMarker);
}


#if 0
void RemoteFDB::doBlockingFlush() {

    connect();

    MessageHeader msg(Message::Flush);
    clientWrite(&msg, sizeof(msg));
    clientWrite(&EndMarker, sizeof(EndMarker));

    MessageHeader response;
    eckit::FixedString<4> tail;

    clientRead(&response, sizeof(MessageHeader));

    handleError(response);

    ASSERT(response.marker == StartMarker);
    ASSERT(response.version == CurrentVersion);
    ASSERT(response.message == Message::Complete);

    clientRead(&tail, 4);
    ASSERT(tail == EndMarker);
}
#endif
#endif


void RemoteFDB::print(std::ostream &s) const {
    s << "RemoteFDB(host=" << hostname_ << ", port=" << port_ << ", dataport=" << dataport_ << ")";
}


//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
