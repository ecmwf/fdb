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

#include "metkit/mars/MarsRequest.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/database/Key.h"
#include "fdb5/remote/AvailablePortList.h"
#include "fdb5/remote/Handler.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/remote/RemoteFieldLocation.h"

using namespace eckit;
using metkit::mars::MarsRequest;

namespace fdb5 {
namespace remote {

//----------------------------------------------------------------------------------------------------------------------

namespace {
class TCPException : public Exception {
public:
    TCPException(const std::string& msg, const CodeLocation& here) :
        Exception(std::string("TCPException: ") + msg, here) {}
};
}  // namespace

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
    virtual size_t encodeBufferSize(const ValueType&) const { return 4096; }
    void extraDecode(eckit::Stream&) {}
    ValueType apiCall(FDB&, const FDBToolRequest&) const { NOTIMP; }

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
        ListElement updated(elem.keyParts_, std::make_shared<RemoteFieldLocation>(
                                                *elem.location_, handler.host(), handler.port()));

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
    void extraDecode(eckit::Stream& s) {
        s >> doit_;
        s >> porcelain_;
    }

    PurgeIterator apiCall(FDB& fdb, const FDBToolRequest& request) const {
        return fdb.purge(request, doit_, porcelain_);
    }

private:
    bool doit_;
    bool porcelain_;
};


struct StatsHelper : public BaseHelper<StatsElement> {
    StatsIterator apiCall(FDB& fdb, const FDBToolRequest& request) const {
        return fdb.stats(request);
    }
};

struct StatusHelper : public BaseHelper<StatusElement> {
    StatusIterator apiCall(FDB& fdb, const FDBToolRequest& request) const {
        return fdb.status(request);
    }
};

struct WipeHelper : public BaseHelper<WipeElement> {
    void extraDecode(eckit::Stream& s) {
        s >> doit_;
        s >> porcelain_;
        s >> unsafeWipeAll_;
    }

    WipeIterator apiCall(FDB& fdb, const FDBToolRequest& request) const {
        return fdb.wipe(request, doit_, porcelain_, unsafeWipeAll_);
    }

private:
    bool doit_;
    bool porcelain_;
    bool unsafeWipeAll_;
};

struct ControlHelper : public BaseHelper<ControlElement> {
    void extraDecode(eckit::Stream& s) {
        s >> action_;
        identifiers_ = ControlIdentifiers(s);
    }

    ControlIterator apiCall(FDB& fdb, const FDBToolRequest& request) const {
        return fdb.control(request, action_, identifiers_);
    }

private:
    ControlAction action_;
    ControlIdentifiers identifiers_;
};

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

// n.b. by default the retrieve queue is big -- we are only queueing the requests, and it
// is a common idiom to queue _many_ requests behind each other (and then aggregate the
// results in a MultiHandle/HandleGatherer).

RemoteHandler::RemoteHandler(eckit::net::TCPSocket& socket, const Config& config) :
    config_(config),
    controlSocket_(socket),
    dataSocket_(selectDataPort()),
    dataListenHostname_(config.getString("dataListenHostname", "")),
    fdb_(config),
    retrieveQueue_(eckit::Resource<size_t>("fdbRetrieveQueueSize", 10000)) {}

RemoteHandler::~RemoteHandler() {
    // We don't want to die before the worker threads are cleaned up

    waitForWorkers();

    // And notify the client that we are done.

    Log::info() << "Sending exit message to client" << std::endl;
    dataWrite(Message::Exit, 0);
    Log::info() << "Done" << std::endl;
}

void RemoteHandler::initialiseConnections() {
    // Read the startup message from the client. Check that it all checks out.

    MessageHeader hdr;
    socketRead(&hdr, sizeof(hdr), controlSocket_);

    ASSERT(hdr.marker == StartMarker);
    ASSERT(hdr.version == CurrentVersion);
    ASSERT(hdr.message == Message::Startup);
    ASSERT(hdr.requestID == 0);

    Buffer payload1 = receivePayload(hdr, controlSocket_);
    eckit::FixedString<4> tail;
    socketRead(&tail, sizeof(tail), controlSocket_);
    ASSERT(tail == EndMarker);

    MemoryStream s1(payload1);
    SessionID clientSession(s1);
    net::Endpoint endpointFromClient(s1);
    LocalConfiguration clientAvailableFunctionality(s1);

    // We want a data connection too. Send info to RemoteFDB, and wait for connection
    // n.b. FDB-192: we use the host communicated from the client endpoint. This
    //               ensures that if a specific interface has been selected and the
    //               server has multiple, then we use that on, whilst retaining
    //               the capacity in the protocol for the server to make a choice.

    int dataport = dataSocket_.localPort();
    // std::string host = dataListenHostname_.empty() ? dataSocket_.localHost() :
    // dataListenHostname_;
    net::Endpoint dataEndpoint(endpointFromClient.hostname(), dataport);

    Log::info() << "Sending data endpoint to client: " << dataEndpoint << std::endl;

    {
        Buffer startupBuffer(1024);
        MemoryStream s(startupBuffer);

        s << clientSession;
        s << sessionID_;
        s << dataEndpoint;

        // TODO: Function to decide what functionality we will actually use. This just
        //       sets up the components of the over-the-wire protocol
        s << LocalConfiguration().get();

        controlWrite(Message::Startup, 0, startupBuffer.data(), s.position());
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

    Buffer payload2 = receivePayload(dataHdr, dataSocket_);
    socketRead(&tail, sizeof(tail), dataSocket_);
    ASSERT(tail == EndMarker);

    MemoryStream s2(payload2);
    SessionID clientSession2(s2);
    SessionID serverSession(s2);

    if (clientSession != clientSession2) {
        std::stringstream ss;
        ss << "Client session IDs do not match: " << clientSession << " != " << clientSession2;
        throw BadValue(ss.str(), Here());
    }

    if (serverSession != sessionID_) {
        std::stringstream ss;
        ss << "Session IDs do not match: " << serverSession << " != " << sessionID_;
        throw BadValue(ss.str(), Here());
    }
}

void RemoteHandler::handle() {
    initialiseConnections();

    // And go into the main loop

    Log::info() << "Server started ..." << std::endl;

    MessageHeader hdr;
    eckit::FixedString<4> tail;

    while (true) {
        tidyWorkers();

        socketRead(&hdr, sizeof(hdr), controlSocket_);

        ASSERT(hdr.marker == StartMarker);
        ASSERT(hdr.version == CurrentVersion);
        Log::debug<LibFdb5>() << "Got message with request ID: " << hdr.requestID << std::endl;

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

                case Message::Status:
                    forwardApiCall<StatusHelper>(hdr);
                    break;

                case Message::Wipe:
                    forwardApiCall<WipeHelper>(hdr);
                    break;

                case Message::Control:
                    forwardApiCall<ControlHelper>(hdr);
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
                    ss << "ERROR: Unexpected message recieved (" << static_cast<int>(hdr.message)
                       << "). ABORTING";
                    Log::status() << ss.str() << std::endl;
                    Log::error() << "Retrieving... " << ss.str() << std::endl;
                    throw SeriousBug(ss.str(), Here());
                }
            }

            // Ensure we have consumed exactly the correct amount from the socket.

            socketRead(&tail, sizeof(tail), controlSocket_);
            ASSERT(tail == EndMarker);

            // Acknowledge receipt of command

            controlWrite(Message::Received, hdr.requestID);
        }
        catch (std::exception& e) {
            // n.b. more general than eckit::Exception
            std::string what(e.what());
            controlWrite(Message::Error, hdr.requestID, what.c_str(), what.length());
        }
        catch (...) {
            std::string what("Caught unexpected and unknown error");
            controlWrite(Message::Error, hdr.requestID, what.c_str(), what.length());
        }
    }
}

int RemoteHandler::selectDataPort() {
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

void RemoteHandler::controlWrite(Message msg, uint32_t requestID, const void* payload,
                                 uint32_t payloadLength) {
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

void RemoteHandler::socketRead(void* data, size_t length, eckit::net::TCPSocket& socket) {
    size_t read = socket.read(data, length);
    if (length != read) {
        std::stringstream ss;
        ss << "Read error. Expected " << length << " bytes, read " << read;
        throw TCPException(ss.str(), Here());
    }
}

void RemoteHandler::dataWrite(Message msg, uint32_t requestID, const void* payload,
                              uint32_t payloadLength) {
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


Buffer RemoteHandler::receivePayload(const MessageHeader& hdr, net::TCPSocket& socket) {
    Buffer payload(hdr.payloadSize);

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
        }
        else {
            ++it;
        }
    }
}

void RemoteHandler::waitForWorkers() {
    retrieveQueue_.close();

    tidyWorkers();

    for (auto& it : workerThreads_) {
        Log::error() << "Worker thread still alive for request ID: " << it.first << std::endl;
        Log::error() << "Joining ..." << std::endl;
        it.second.get();
        Log::error() << "Thread complete" << std::endl;
    }

    if (retrieveWorker_.joinable()) {
        retrieveWorker_.join();
    }
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

    workerThreads_.emplace(
        hdr.requestID, std::async(std::launch::async, [request, hdr, helper, this]() {
            try {
                auto iterator = helper.apiCall(fdb_, request);

                typename decltype(iterator)::value_type elem;
                while (iterator.next(elem)) {
                    auto encoded(helper.encode(elem, *this));
                    dataWrite(Message::Blob, hdr.requestID, encoded.buf, encoded.position);
                }

                dataWrite(Message::Complete, hdr.requestID);
            }
            catch (std::exception& e) {
                // n.b. more general than eckit::Exception
                std::string what(e.what());
                dataWrite(Message::Error, hdr.requestID, what.c_str(), what.length());
            }
            catch (...) {
                // We really don't want to std::terminate the thread
                std::string what("Caught unexpected, unknown exception in worker");
                dataWrite(Message::Error, hdr.requestID, what.c_str(), what.length());
            }
        }));
}


void RemoteHandler::archive(const MessageHeader& hdr) {
    ASSERT(hdr.payloadSize == 0);

    // Ensure that we aren't already running an archive()

    ASSERT(!archiveFuture_.valid());

    // Start archive worker thread

    uint32_t id    = hdr.requestID;
    archiveFuture_ = std::async(std::launch::async, [this, id] { return archiveThreadLoop(id); });
}


// A helper function to make archiveThreadLoop a bit cleaner

static void archiveBlobPayload(FDB& fdb, const void* data, size_t length) {
    MemoryStream s(data, length);

    fdb5::Key key(s);

    std::stringstream ss_key;
    ss_key << key;

    const char* charData = static_cast<const char*>(data);  // To allow pointer arithmetic
    Log::status() << "Archiving data: " << ss_key.str() << std::endl;
    fdb.archive(key, charData + s.position(), length - s.position());
    Log::status() << "Archiving done: " << ss_key.str() << std::endl;
}


size_t RemoteHandler::archiveThreadLoop(uint32_t id) {
    size_t totalArchived = 0;

    // Create a worker that will do the actual archiving

    static size_t queueSize(eckit::Resource<size_t>("fdbServerMaxQueueSize", 32));
    eckit::Queue<std::pair<eckit::Buffer, bool>> queue(queueSize);

    std::future<size_t> worker = std::async(std::launch::async, [this, &queue, id] {
        size_t totalArchived = 0;

        std::pair<eckit::Buffer, bool> elem = std::make_pair(Buffer{0}, false);

        try {
            long queuelen;
            while ((queuelen = queue.pop(elem)) != -1) {
                if (elem.second) {
                    // Handle MultiBlob

                    const char* firstData =
                        static_cast<const char*>(elem.first.data());  // For pointer arithmetic
                    const char* charData = firstData;
                    while (size_t(charData - firstData) < elem.first.size()) {
                        const MessageHeader* hdr =
                            static_cast<const MessageHeader*>(static_cast<const void*>(charData));
                        ASSERT(hdr->marker == StartMarker);
                        ASSERT(hdr->version == CurrentVersion);
                        ASSERT(hdr->message == Message::Blob);
                        ASSERT(hdr->requestID == id);
                        charData += sizeof(MessageHeader);

                        const void* payloadData = charData;
                        charData += hdr->payloadSize;

                        const decltype(EndMarker)* e = static_cast<const decltype(EndMarker)*>(
                            static_cast<const void*>(charData));
                        ASSERT(*e == EndMarker);
                        charData += sizeof(EndMarker);

                        archiveBlobPayload(fdb_, payloadData, hdr->payloadSize);
                        totalArchived += 1;
                    }
                }
                else {
                    // Handle single blob
                    archiveBlobPayload(fdb_, elem.first.data(), elem.first.size());
                    totalArchived += 1;
                }
            }
        }
        catch (...) {
            // Ensure exception propagates across the queue back to the parent thread.
            queue.interrupt(std::current_exception());
            throw;
        }

        return totalArchived;
    });

    try {
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
            if (hdr.message == Message::Flush)
                break;

            ASSERT(hdr.message == Message::Blob || hdr.message == Message::MultiBlob);

            Buffer payload(receivePayload(hdr, dataSocket_));
            MemoryStream s(payload);

            eckit::FixedString<4> tail;
            socketRead(&tail, sizeof(tail), dataSocket_);
            ASSERT(tail == EndMarker);

            // Queueing payload

            size_t sz = payload.size();
            Log::debug<LibFdb5>() << "Queueing data: " << sz << std::endl;
            size_t queuelen = queue.emplace(
                std::make_pair(std::move(payload), hdr.message == Message::MultiBlob));
            Log::status() << "Queued data (" << queuelen << ", size=" << sz << ")" << std::endl;
            ;
            Log::debug<LibFdb5>() << "Queued data (" << queuelen << ", size=" << sz << ")"
                                  << std::endl;
            ;
        }

        // Trigger cleanup of the workers
        queue.close();

        // Complete reading the Flush instruction

        eckit::FixedString<4> tail;
        socketRead(&tail, sizeof(tail), dataSocket_);
        ASSERT(tail == EndMarker);

        // Ensure worker is done

        ASSERT(worker.valid());
        totalArchived = worker.get();  // n.b. use of async, get() propagates any exceptions.
    }
    catch (std::exception& e) {
        // n.b. more general than eckit::Exception
        std::string what(e.what());
        dataWrite(Message::Error, id, what.c_str(), what.length());
        queue.interrupt(std::current_exception());
        throw;
    }
    catch (...) {
        std::string what("Caught unexpected, unknown exception in retrieve worker");
        dataWrite(Message::Error, id, what.c_str(), what.length());
        queue.interrupt(std::current_exception());
        throw;
    }

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
            Log::debug<LibFdb5>() << "Retrieving (" << requestID << ")" << request << std::endl;

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

            Log::debug<LibFdb5>() << "Writing retrieve complete message: " << requestID
                                  << std::endl;

            dataWrite(Message::Complete, requestID);

            Log::status() << "Done retrieve: " << requestID << std::endl;
            Log::debug<LibFdb5>() << "Done retrieve: " << requestID << std::endl;
        }
        catch (std::exception& e) {
            // n.b. more general than eckit::Exception
            std::string what(e.what());
            dataWrite(Message::Error, requestID, what.c_str(), what.length());
        }
        catch (...) {
            // We really don't want to std::terminate the thread
            std::string what("Caught unexpected, unknown exception in retrieve worker");
            dataWrite(Message::Error, requestID, what.c_str(), what.length());
        }
    }
}


//----------------------------------------------------------------------------------------------------------------------

}  // namespace remote
}  // namespace fdb5
