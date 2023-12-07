/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/config/Resource.h"
#include "eckit/serialisation/MemoryStream.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Store.h"
#include "fdb5/remote/server/StoreHandler.h"

using namespace eckit;
using metkit::mars::MarsRequest;

namespace fdb5 {
namespace remote {

StoreHandler::StoreHandler(eckit::net::TCPSocket& socket, const Config& config):
    ServerConnection(socket, config) {}

StoreHandler::~StoreHandler() {}

void StoreHandler::initialiseConnections() {
    ServerConnection::initialiseConnections();

    Log::info() << "StoreHandler::initialiseConnections" << std::endl;
}


void StoreHandler::handle() {
    initialiseConnections();


    Log::info() << "Server started ..." << std::endl;

    MessageHeader hdr;
    eckit::FixedString<4> tail;

    // listen loop
    while (true) {
        tidyWorkers();

        socketRead(&hdr, sizeof(hdr), controlSocket_);

        ASSERT(hdr.marker == StartMarker);
        ASSERT(hdr.version == CurrentVersion);
        Log::debug<LibFdb5>() << "StoreHandler - got message " << ((int) hdr.message) << " with request ID: " << hdr.requestID << std::endl;

        try {
            switch (hdr.message) {
                case Message::Exit:
                    Log::status() << "Exiting" << std::endl;
                    Log::info() << "Exiting" << std::endl;
                    return;

                case Message::List:
                    NOTIMP;
                    break;

                case Message::Dump:
                    NOTIMP;
                    break;

                case Message::Purge:
                    NOTIMP;
                    break;

                case Message::Stats:
                    NOTIMP;
                    break;

                case Message::Status:
                    NOTIMP;
                    break;

                case Message::Wipe:
                    NOTIMP;
                    break;

                case Message::Control:
                    NOTIMP;
                    break;

                case Message::Inspect:
                    NOTIMP;
                    break;

                case Message::Read: 
                    read(hdr);
                    break;

                case Message::Flush:
                    flush(hdr);
                    break;

                case Message::Archive:
                    archive(hdr);
                    break;

                default: {
                    std::stringstream ss;
                    ss << "ERROR: Unexpected message recieved (" << static_cast<int>(hdr.message)
                       << "). ABORTING";
                    Log::status() << ss.str() << std::endl;
                    Log::error() << "Store Retrieving... " << ss.str() << std::endl;
                    throw SeriousBug(ss.str(), Here());
                }
            }

            // Ensure we have consumed exactly the correct amount from the socket.
            socketRead(&tail, sizeof(tail), controlSocket_);
            ASSERT(tail == EndMarker);

            // Acknowledge receipt of command
            dataWrite(Message::Received, hdr.clientID, hdr.requestID);
        }
        catch (std::exception& e) {
            // n.b. more general than eckit::Exception
            std::string what(e.what());
            dataWrite(Message::Error, hdr.clientID, hdr.requestID, what.c_str(), what.length());
        }
        catch (...) {
            std::string what("Caught unexpected and unknown error");
            dataWrite(Message::Error, hdr.clientID, hdr.requestID, what.c_str(), what.length());
        }
    }
}

void StoreHandler::read(const MessageHeader& hdr) {

    if (!readLocationWorker_.joinable()) {
        readLocationWorker_ = std::thread([this] { readLocationThreadLoop(); });
    }

    Buffer payload(receivePayload(hdr, controlSocket_));
    MemoryStream s(payload);

    std::unique_ptr<FieldLocation> location(eckit::Reanimator<FieldLocation>::reanimate(s));

    Log::debug<LibFdb5>() << "Queuing for read: " << hdr.requestID << " " << *location << std::endl;

    std::unique_ptr<eckit::DataHandle> dh;
    dh.reset(location->dataHandle());

    readLocationQueue_.emplace(readLocationElem(hdr.clientID, hdr.requestID, std::move(dh)));
}

void StoreHandler::readLocationThreadLoop() {
    readLocationElem elem;

    while (readLocationQueue_.pop(elem) != -1) {
        // Get the next MarsRequest in sequence to work on, do the retrieve, and
        // send the data back to the client.

        // Forward the API call
        writeToParent(elem.clientID, elem.requestID, std::move(elem.readLocation));
    }
}

void StoreHandler::writeToParent(const uint32_t clientID, const uint32_t requestID, std::unique_ptr<eckit::DataHandle> dh) {
   try {
        Log::status() << "Reading: " << requestID << std::endl;
        // Write the data to the parent, in chunks if necessary.

        Buffer writeBuffer(10 * 1024 * 1024);
        long dataRead;

        dh->openForRead();
        Log::debug<LibFdb5>() << "Reading: " << requestID << " dh size: " << dh->size()
                              << std::endl;

        while ((dataRead = dh->read(writeBuffer, writeBuffer.size())) != 0) {
            dataWrite(Message::Blob, clientID, requestID, writeBuffer, dataRead);
        }

        // And when we are done, add a complete message.

        Log::debug<LibFdb5>() << "Writing retrieve complete message: " << requestID
                              << std::endl;

        dataWrite(Message::Complete, clientID, requestID);

        Log::status() << "Done retrieve: " << requestID << std::endl;
        Log::debug<LibFdb5>() << "Done retrieve: " << requestID << std::endl;
    }
    catch (std::exception& e) {
        // n.b. more general than eckit::Exception
        std::string what(e.what());
        dataWrite(Message::Error, clientID, requestID, what.c_str(), what.length());
    }
    catch (...) {
        // We really don't want to std::terminate the thread
        std::string what("Caught unexpected, unknown exception in retrieve worker");
        dataWrite(Message::Error, clientID, requestID, what.c_str(), what.length());
    }
}

// void StoreHandler::archive(const MessageHeader& hdr) {

//     ASSERT(hdr.payloadSize == 0);

//     auto archive = archiveFuture_.find(hdr.clientID());

//     // Ensure that we aren't already running a store()
//     if(archive == archiveFuture_.end() || !archive->second.valid())
//         // Start archive worker thread
//         archiveFuture_[hdr.clientID()] = std::async(std::launch::async, [this] { return archiveThreadLoop(); });
//     }
// }

Store& StoreHandler::store(uint32_t id, Key dbKey) {
    auto it = stores_.find(id);
    if (it != stores_.end()) {
        return *(it->second);
    }

    return *(stores_[id] = StoreFactory::instance().build(dbKey, config_));
}

// A helper function to make archiveThreadLoop a bit cleaner
void StoreHandler::archiveBlobPayload(const uint32_t archiverID, const uint32_t clientID, const uint32_t requestID, const void* data, size_t length) {
    MemoryStream s(data, length);

    fdb5::Key dbKey(s);
    fdb5::Key idxKey(s);

    std::stringstream ss_key;
    ss_key << dbKey << idxKey;

    const char* charData = static_cast<const char*>(data);  // To allow pointer arithmetic
    Log::status() << "Archiving data: " << ss_key.str() << std::endl;
    
    Store& ss = store(clientID, dbKey);

    auto futureLocation = ss.archive(archiverID, idxKey, charData + s.position(), length - s.position());
    Log::status() << "Archiving done: " << ss_key.str() << std::endl;
    
    auto loc = futureLocation.get();

//    ss.flush();

    eckit::Buffer locBuffer(16 * 1024);
    MemoryStream locStream(locBuffer);
    locStream << (*loc);
    dataWrite(Message::Archive, clientID, requestID, locBuffer.data(), locStream.position());
}

struct archiveElem {
    uint32_t clientID;
    uint32_t requestID;
    eckit::Buffer payload;
    bool multiblob;

    archiveElem() : clientID(0), requestID(0), payload(0), multiblob(false) {}

    archiveElem(uint32_t clientID, uint32_t requestID, eckit::Buffer payload, bool multiblob) :
        clientID(clientID), requestID(requestID), payload(std::move(payload)), multiblob(multiblob) {}
};

size_t StoreHandler::archiveThreadLoop(uint32_t archiverID) {
    size_t totalArchived = 0;

    // Create a worker that will do the actual archiving

    static size_t queueSize(eckit::Resource<size_t>("fdbServerMaxQueueSize", 32));
    eckit::Queue<archiveElem> queue(queueSize);

    std::future<size_t> worker = std::async(std::launch::async, [this, &queue, archiverID] {
        size_t totalArchived = 0;

        archiveElem elem;

        try {
            long queuelen;
            while ((queuelen = queue.pop(elem)) != -1) {
                if (elem.multiblob) {
                    // Handle MultiBlob

                    const char* firstData = static_cast<const char*>(elem.payload.data());  // For pointer arithmetic
                    const char* charData = firstData;
                    while (size_t(charData - firstData) < elem.payload.size()) {
                        const MessageHeader* hdr = static_cast<const MessageHeader*>(static_cast<const void*>(charData));
                        ASSERT(hdr->marker == StartMarker);
                        ASSERT(hdr->version == CurrentVersion);
                        ASSERT(hdr->message == Message::Blob);
                        ASSERT(hdr->clientID == elem.clientID);
                        ASSERT(hdr->requestID == elem.requestID);
                        charData += sizeof(MessageHeader);

                        const void* payloadData = charData;
                        charData += hdr->payloadSize;

                        const decltype(EndMarker)* e = static_cast<const decltype(EndMarker)*>(static_cast<const void*>(charData));
                        ASSERT(*e == EndMarker);
                        charData += sizeof(EndMarker);

                        archiveBlobPayload(archiverID, elem.clientID, elem.requestID, payloadData, hdr->payloadSize);
                        totalArchived += 1;
                    }
                }
                else {
                    // Handle single blob
                    archiveBlobPayload(archiverID, elem.clientID, elem.requestID, elem.payload.data(), elem.payload.size());
                    totalArchived += 1;
                }
            }
            
            // dataWrite(Message::Complete, 0);
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
            //ASSERT(hdr.requestID == id);

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
            size_t queuelen = queue.emplace(archiveElem(hdr.clientID, hdr.requestID, std::move(payload), hdr.message == Message::MultiBlob));
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
        dataWrite(Message::Error, 0, 0, what.c_str(), what.length());
        queue.interrupt(std::current_exception());
        throw;
    }
    catch (...) {
        std::string what("Caught unexpected, unknown exception in retrieve worker");
        dataWrite(Message::Error, 0, 0, what.c_str(), what.length());
        queue.interrupt(std::current_exception());
        throw;
    }

    return totalArchived;
}

void StoreHandler::flush(const MessageHeader& hdr) {
    Buffer payload(receivePayload(hdr, controlSocket_));
    MemoryStream s(payload);

    size_t numArchived;
    s >> numArchived;

    std::future<size_t>& archive = archiveFuture_[hdr.clientID];
    // std::cout << "numArchived: " << numArchived << " | archiveFuture_.valid: " << archive.valid() << std::endl;

    ASSERT(numArchived == 0 || archive.valid());

    if (archive.valid()) {
        // Ensure that the expected number of fields have been written, and that the
        // archive worker processes have been cleanly wound up.
        size_t n = archive.get();
        ASSERT(numArchived == n);

        // Do the actual flush!
        Log::info() << "Flushing" << std::endl;
        Log::status() << "Flushing" << std::endl;
        stores_[hdr.clientID]->flush();
        // for (auto it = stores_.begin(); it != stores_.end(); it++) {
        //     it->second->flush();
        // }
    }

    Log::info() << "Flush complete" << std::endl;
    Log::status() << "Flush complete" << std::endl;
}

}  // namespace remote
}  // namespace fdb5
