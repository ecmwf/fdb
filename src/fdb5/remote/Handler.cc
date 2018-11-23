/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/remote/Handler.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/remote/RemoteFieldLocation.h"
#include "fdb5/database/Key.h"
#include "fdb5/LibFdb.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

#include "marslib/MarsRequest.h"

#include "eckit/maths/Functions.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/config/Resource.h"

#include <chrono>

using namespace eckit;


namespace fdb5 {
namespace remote {

//----------------------------------------------------------------------------------------------------------------------

class TCPException : public Exception {
public:
    TCPException(const std::string& msg, const CodeLocation& here) :
        Exception(std::string("TCPException: ") + msg, here) {}
};

//----------------------------------------------------------------------------------------------------------------------

// ***************************************************************************************
// All of the standard API functions behave in roughly the same manner. The Helper Classes
// described here capture the manner in which their behaviours differ.
//
// See forwardApiCall() for how these helpers are used to forward an API call using a
// worker thread.
//
// ***************************************************************************************
//
// Note that we use the "control" and "data" connections in a specific way, although these
// may not be the optimal names for them. "control" is used for blocking requests,
// and "data" is used for non-blocking activity.
//
// ***************************************************************************************

namespace {

template <typename ValueType>
struct BaseHelper {
    static size_t encodeBufferSize(const ValueType&) { return 4096; }
    void extraDecode(eckit::Stream&) {}
    ValueType apiCall(FDB& fdb, const FDBToolRequest&) const { NOTIMP; }

    struct Encoded {
        size_t position;
        eckit::Buffer buf;
    };

    Encoded encode(const ValueType& elem, const RemoteHandler&) const {
        eckit::Buffer encodeBuffer(encodeBufferSize(elem));
        MemoryStream s(encodeBuffer);
        s << elem;
        return {s.position(), std::move(encodeBuffer)};
    }
};


struct ListHelper : public BaseHelper<ListElement> {

    ListIterator apiCall(FDB& fdb, const FDBToolRequest& request) const {
        return fdb.list(request);
    }

    // Create a derived RemoteFieldLocation which knows about this server
    Encoded encode(const ListElement& elem, const RemoteHandler& handler) const {

        ListElement updated(elem.keyParts_,
                            std::make_shared<RemoteFieldLocation>(*elem.location_, handler.host(), handler.port()));

        return BaseHelper<ListElement>::encode(updated, handler);
    }
};


struct DumpHelper : public BaseHelper<DumpElement> {

    void extraDecode(eckit::Stream& s) { s >> simple_; }

    DumpIterator apiCall(FDB& fdb, const FDBToolRequest& request) const {
        return fdb.dump(request, simple_);
    }

private:
    bool simple_;
};


struct PurgeHelper : public BaseHelper<PurgeElement> {

    void extraDecode(eckit::Stream& s) { s >> doit_; }

    PurgeIterator apiCall(FDB& fdb, const FDBToolRequest& request) const {
        return fdb.purge(request, doit_);
    }

private:
    bool doit_;
};


struct StatsHelper : public BaseHelper<StatsElement> {
    StatsIterator apiCall(FDB& fdb, const FDBToolRequest& request) const {
        return fdb.stats(request);
    }
};

struct WhereHelper : public BaseHelper<WhereElement> {
    WhereIterator apiCall(FDB& fdb, const FDBToolRequest& request) const {
        return fdb.where(request);
    }
};

struct WipeHelper : public BaseHelper<WipeElement> {

    void extraDecode(eckit::Stream& s) { s >> doit_; }

    WipeIterator apiCall(FDB& fdb, const FDBToolRequest& request) const {
        return fdb.wipe(request, doit_);
    }

    static size_t encodeBufferSize(const WipeElement& elem) {
        size_t totalSize = elem.guessEncodedSize();
        return eckit::round(totalSize, 4096);
    }

private:
    bool doit_;
};

} // namespace

//----------------------------------------------------------------------------------------------------------------------

// n.b. by default the retrieve queue is big -- we are only queueing the requests, and it
// is a common idiom to queue _many_ requests behind each other (and then aggregate the
// results in a MultiHandle/HandleGatherer).

RemoteHandler::RemoteHandler(eckit::TCPSocket& socket, const Config& config) :
    controlSocket_(socket),
    dataSocket_(),
    fdb_(config),
    retrieveQueue_(eckit::Resource<size_t>("fdbRetrieveQueueSize", 10000)) {}

RemoteHandler::~RemoteHandler() {

    // We don't want to die before the worker threads are cleaned up

    waitForWorkers();

    // And notify the client that we are done.

    Log::debug<LibFdb>() << "Sending exit message to client" << std::endl;
    dataWrite(Message::Exit, 0);
    Log::debug<LibFdb>() << "Done" << std::endl;
}

void RemoteHandler::handle() {

    // We want a data connection too. Send info to RemoteFDB, and wait for connection

    int dataport = dataSocket_.localPort();

    Log::debug<LibFdb>() << "Sending data port to client: " << dataport << std::endl;

    controlWrite(&dataport, sizeof(dataport));

    dataSocket_.accept();

    // And go into the main loop

    Log::info() << "Server started ..." << std::endl;

    MessageHeader hdr;
    eckit::FixedString<4> tail;

    while (true) {

        tidyWorkers();

        socketRead(&hdr, sizeof(hdr), controlSocket_);

        ASSERT(hdr.marker == StartMarker);
        ASSERT(hdr.version == CurrentVersion);
        Log::debug<LibFdb>() << "Got message with request ID: " << hdr.requestID << std::endl;

        try {

            switch (hdr.message) {

            case Message::Exit:
                Log::status() << "Exiting" << std::endl;
                Log::info() << "Exiting" << std::endl;
                return;

            case Message::List:
                forwardApiCall<ListHelper>(hdr);
                break;

            case Message::Dump:
                forwardApiCall<DumpHelper>(hdr);
                break;

            case Message::Purge:
                forwardApiCall<PurgeHelper>(hdr);
                break;

            case Message::Stats:
                forwardApiCall<StatsHelper>(hdr);
                break;

            case Message::Where:
                forwardApiCall<WhereHelper>(hdr);
                break;

            case Message::Wipe:
                forwardApiCall<WipeHelper>(hdr);
                break;

            case Message::Flush:
                flush(hdr);
                break;

            case Message::Archive:
                archive(hdr);
                break;

            case Message::Retrieve:
                retrieve(hdr);
                break;

            default: {
                std::stringstream ss;
                ss << "ERROR: Unexpected message recieved (" << static_cast<int>(hdr.message) << "). ABORTING";
                Log::status() << ss.str() << std::endl;
                Log::error() << "Retrieving... " << ss.str() << std::endl;
                throw SeriousBug(ss.str(), Here());
            }
            };

            // Ensure we have consumed exactly the correct amount from the socket.

            socketRead(&tail, sizeof(tail), controlSocket_);
            ASSERT(tail == EndMarker);

            // Acknowledge receipt of command

            controlWrite(Message::Received, hdr.requestID);

        } catch (std::exception& e) {
            // n.b. more general than eckit::Exception
            std::string what(e.what());
            controlWrite(Message::Error, hdr.requestID, what.c_str(), what.length());
        } catch (...) {
            std::string what("Caught unexpected and unknown error");
            controlWrite(Message::Error, hdr.requestID, what.c_str(), what.length());
        }
    }
}

void RemoteHandler::controlWrite(Message msg, uint32_t requestID, const void* payload, uint32_t payloadLength) {

    ASSERT((payload == nullptr) == (payloadLength == 0));

    MessageHeader message(msg, requestID, payloadLength);
    controlWrite(&message, sizeof(message));
    if (payload) {
        controlWrite(payload, payloadLength);
    }
    controlWrite(&EndMarker, sizeof(EndMarker));
}

void RemoteHandler::controlWrite(const void* data, size_t length) {

    size_t written = controlSocket_.write(data, length);
    if (length != written) {
        std::stringstream ss;
        ss << "Write error. Expected " << length << " bytes, wrote " << written;
        throw TCPException(ss.str(), Here());
    }
}

void RemoteHandler::socketRead(void* data, size_t length, eckit::TCPSocket& socket) {

    size_t read = socket.read(data, length);
    if (length != read) {
        std::stringstream ss;
        ss << "Read error. Expected " << length << " bytes, read " << read;
        throw TCPException(ss.str(), Here());
    }
}

void RemoteHandler::dataWrite(Message msg, uint32_t requestID, const void* payload, uint32_t payloadLength) {

    ASSERT((payload == nullptr) == (payloadLength == 0));

    MessageHeader message(msg, requestID, payloadLength);

    std::lock_guard<std::mutex> lock(dataWriteMutex_);

    dataWriteUnsafe(&message, sizeof(message));
    if (payload) {
        dataWriteUnsafe(payload, payloadLength);
    }
    dataWriteUnsafe(&EndMarker, sizeof(EndMarker));
}

void RemoteHandler::dataWriteUnsafe(const void* data, size_t length) {
    size_t written = dataSocket_.write(data, length);
    if (length != written) {
        std::stringstream ss;
        ss << "Write error. Expected " << length << " bytes, wrote " << written;
        throw TCPException(ss.str(), Here());
    }
}


Buffer RemoteHandler::receivePayload(const MessageHeader &hdr, TCPSocket& socket) {

    Buffer payload(eckit::round(hdr.payloadSize, 4*1024));

    ASSERT(hdr.payloadSize > 0);
    socketRead(payload, hdr.payloadSize, socket);

    return payload;
}

void RemoteHandler::tidyWorkers() {

    std::map<uint32_t, std::future<void>>::iterator it = workerThreads_.begin();

    for (; it != workerThreads_.end(); /* empty */) {

        std::future_status stat = it->second.wait_for(std::chrono::milliseconds(0));

        if (stat == std::future_status::ready) {
            Log::info() << "Tidying up worker for request ID: " << it->first << std::endl;
            workerThreads_.erase(it++);
        } else {
            ++it;
        }
    }
}

void RemoteHandler::waitForWorkers() {

    retrieveQueue_.set_done();

    tidyWorkers();

    for (auto& it : workerThreads_) {
        Log::error() << "Worker thread still alive for request ID: " << it.first << std::endl;
        Log::error() << "Joining ..." << std::endl;
        it.second.get();
        Log::error() << "Thread complete" << std::endl;
    }

    retrieveWorker_.join();
}


// TODO: This can be an absolutely standard handler based on the ListElement type.

template <typename HelperClass>
void RemoteHandler::forwardApiCall(const MessageHeader& hdr) {

    HelperClass helper;

    Buffer payload(receivePayload(hdr, controlSocket_));
    MemoryStream s(payload);

    FDBToolRequest request(s);
    helper.extraDecode(s);

    // Construct worker thread to feed responses back to client

    ASSERT(workerThreads_.find(hdr.requestID) == workerThreads_.end());

    workerThreads_.emplace(hdr.requestID, std::async(std::launch::async,
        [request, hdr, helper, this]() {

            try {

                auto iterator = helper.apiCall(fdb_, request);

                typename decltype(iterator)::value_type elem;
                while (iterator.next(elem)) {
                    auto encoded(helper.encode(elem, *this));
                    dataWrite(Message::Blob, hdr.requestID, encoded.buf, encoded.position);
                }

                dataWrite(Message::Complete, hdr.requestID);

            } catch (std::exception& e) {
                // n.b. more general than eckit::Exception
                std::string what(e.what());
                dataWrite(Message::Error, hdr.requestID, what.c_str(), what.length());
            } catch (...) {
                // We really don't want to std::terminate the thread
                std::string what("Caught unexpected, unknown exception in worker");
                dataWrite(Message::Error, hdr.requestID, what.c_str(), what.length());
            }
        }
    ));
}


void RemoteHandler::archive(const MessageHeader& hdr) {

    ASSERT(hdr.payloadSize == 0);

    // Ensure that we aren't already running an archive()

    ASSERT(!archiveFuture_.valid());

    // Start archive worker thread

    uint32_t id = hdr.requestID;
    archiveFuture_ = std::async(std::launch::async, [this, id] { return archiveThreadLoop(id); });
}

size_t RemoteHandler::archiveThreadLoop(uint32_t id) {

    size_t totalArchived = 0;

    Log::info() << "Inside archiveThreadLoop. ID: " << id << std::endl;

    // Create a worker that will do the actual archiving

    static size_t queueSize(eckit::Resource<size_t>("fdbServerMaxQueueSize", 32));
    eckit::Queue<std::pair<fdb5::Key, eckit::Buffer>> queue(queueSize);

    std::future<void> worker = std::async(std::launch::async, [this, &queue] {

        std::pair<fdb5::Key, eckit::Buffer> elem {{}, 0};

        long queuelen;
        while ((queuelen = queue.pop(elem)) != -1) {

            const Key& key(elem.first);
            const Buffer& buffer(elem.second);

            std::stringstream ss_key;
            ss_key << key;
            Log::info() << "Archive (" << queuelen << "): " << ss_key.str() << std::endl;
            Log::status() << "Archive (" << queuelen << "): " << ss_key.str() << std::endl;
            fdb_.archive(key, buffer, buffer.size());
            Log::status() << "Archive done (" << queuelen << "): " << ss_key.str() << std::endl;
        }
    });

    // The archive loop is the only thing that can listen on the data socket,
    // so we don't need to to anything clever here.

    // n.b. we also don't need to lock on read. We are the only thing that reads.

    while (true) {

        MessageHeader hdr;
        socketRead(&hdr, sizeof(hdr), dataSocket_);

        ASSERT(hdr.marker == StartMarker);
        ASSERT(hdr.version == CurrentVersion);
        ASSERT(hdr.requestID == id);

        // Have we been told that we are done yet?
        if (hdr.message == Message::Flush) break;

        ASSERT(hdr.message == Message::Blob);

        Buffer payload(receivePayload(hdr, dataSocket_));
        MemoryStream s(payload);

        eckit::FixedString<4> tail;
        socketRead(&tail, sizeof(tail), dataSocket_);
        ASSERT(tail == EndMarker);

        // Get the key and the data out

        fdb5::Key key(s);

        std::stringstream ss_key;
        ss_key << key;
        Log::status() << "Queueing: " << ss_key.str() << std::endl;
        Log::debug<LibFdb>() << "Queueing: " << ss_key.str() << std::endl;

        // Queue the data for asynchronous handling

        size_t pos = s.position();
        size_t len = hdr.payloadSize - pos;

        size_t queuelen = queue.emplace(std::make_pair(std::move(key), Buffer(&payload[pos], len)));

        Log::status() << "Queued (" << queuelen << "): " << ss_key.str() << std::endl;
        Log::debug<LibFdb>() << "Queued (" << queuelen << "): " << ss_key.str() << std::endl;

        totalArchived += 1;
    }

    // Trigger cleanup of the workers
    queue.set_done();

    // Complete reading the Flush instruction

    eckit::FixedString<4> tail;
    socketRead(&tail, sizeof(tail), dataSocket_);
    ASSERT(tail == EndMarker);

    // Ensure worker is done

    ASSERT(worker.valid());
    worker.get(); // n.b. use of async, get() propagates any exceptions.

    Log::info() << "Recieved end marker. Exiting" << std::endl;

    return totalArchived;
}

void RemoteHandler::flush(const MessageHeader& hdr) {

    Buffer payload(receivePayload(hdr, controlSocket_));
    MemoryStream s(payload);

    size_t numArchived;
    s >> numArchived;

    ASSERT(numArchived == 0 || archiveFuture_.valid());

    if (archiveFuture_.valid()) {

        // Ensure that the expected number of fields have been written, and that the
        // archive worker processes have been cleanly wound up.
        size_t n = archiveFuture_.get();
        ASSERT(numArchived == n);

        // Do the actual flush!
        Log::info() << "Flushing" << std::endl;
        Log::status() << "Flushing" << std::endl;
        fdb_.flush();
        Log::info() << "Flush complete" << std::endl;
        Log::status() << "Flush complete" << std::endl;
    }
}


void RemoteHandler::retrieve(const MessageHeader& hdr) {

    // If we have never done any retrieving before, then start the appropriate
    // worker thread.

    if (!retrieveWorker_.joinable()) {
        retrieveWorker_ = std::thread([this] { retrieveThreadLoop(); });
    }

    Buffer payload(receivePayload(hdr, controlSocket_));
    MemoryStream s(payload);

    MarsRequest request(s);

    retrieveQueue_.emplace(std::make_pair(hdr.requestID, std::move(request)));
}


void RemoteHandler::retrieveThreadLoop() {

    std::pair<uint32_t, MarsRequest> elem;

    while (retrieveQueue_.pop(elem) != -1) {

        // Get the next MarsRequest in sequence to work on, do the retrieve, and
        // send the data back to the client.

        const uint32_t requestID(elem.first);
        const MarsRequest& request(elem.second);

        try {

            Log::status() << "Retrieving: " << requestID << std::endl;
            Log::debug<LibFdb>() << "Retrieving (" << requestID << ")" << request << std::endl;

            // Forward the API call

            std::unique_ptr<eckit::DataHandle> dh(fdb_.retrieve(request));

            // Write the data to the parent, in chunks if necessary.

            Buffer writeBuffer(10 * 1024 * 1024);
            long dataRead;

            dh->openForRead();
            while ((dataRead = dh->read(writeBuffer, writeBuffer.size())) != 0) {
                dataWrite(Message::Blob, requestID, writeBuffer, dataRead);
            }

            // And when we are done, add a complete message.

            Log::debug<LibFdb>() << "Writing retrieve complete message: " << requestID << std::endl;

            dataWrite(Message::Complete, requestID);

            Log::status() << "Done retrieve: " << requestID << std::endl;
            Log::debug<LibFdb>() << "Done retrieve: " << requestID << std::endl;

        } catch (std::exception& e) {
            // n.b. more general than eckit::Exception
            std::string what(e.what());
            dataWrite(Message::Error, requestID, what.c_str(), what.length());
        } catch (...) {
            // We really don't want to std::terminate the thread
            std::string what("Caught unexpected, unknown exception in retrieve worker");
            dataWrite(Message::Error, requestID, what.c_str(), what.length());
        }
    }
}


//----------------------------------------------------------------------------------------------------------------------

} // namespace remote
} // namespace fdb5

