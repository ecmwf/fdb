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
#include "fdb5/api/FDBFactory.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/remote/server/CatalogueHandler.h"

using namespace eckit;
using metkit::mars::MarsRequest;

namespace fdb5::remote {


// ***************************************************************************************
//
// Note that we use the "control" and "data" connections in a specific way, although these
// may not be the optimal names for them. "control" is used for blocking requests,
// and "data" is used for non-blocking activity.
//
// ***************************************************************************************

CatalogueHandler::CatalogueHandler(eckit::net::TCPSocket& socket, const Config& config):
    ServerConnection(socket, config) {

}
CatalogueHandler::~CatalogueHandler() {}

void CatalogueHandler::initialiseConnections() {
    ServerConnection::initialiseConnections();

    MessageHeader hdr;
    eckit::FixedString<4> tail;

    socketRead(&hdr, sizeof(hdr), controlSocket_);

    ASSERT(hdr.marker == StartMarker);
    ASSERT(hdr.version == CurrentVersion);
    ASSERT(hdr.message == Message::MasterSchema);

    // Ensure we have consumed exactly the correct amount from the socket.
    socketRead(&tail, sizeof(tail), controlSocket_);
    ASSERT(tail == EndMarker);

    std::vector<eckit::net::Endpoint> stores;

    if (::getenv("FDB_STORE_HOST") && ::getenv("FDB_STORE_PORT")) {
        // override the configuration
        stores.push_back(net::Endpoint(::getenv("FDB_STORE_HOST"), std::stoi(::getenv("FDB_STORE_PORT"))));
    }
    else {
        std::vector<std::string> endpoints = config_.getStringVector("stores");
        for (const std::string& endpoint: endpoints) {
            stores.push_back(eckit::net::Endpoint(endpoint));
        }
    }

    ASSERT(stores.size() > 0);
    // TODO randomise
    eckit::net::Endpoint endpoint = stores.at(0);

    Log::info() << "Sending store endpoint to client: " << endpoint << std::endl;
    {
        Buffer startupBuffer(102400);
        MemoryStream s(startupBuffer);

        // s << clientSession;
        // s << sessionID_;
        s << endpoint;
        s << config_.schema();

//        Log::debug() << "Protocol negotiation - configuration: " << agreedConf_ <<std::endl;

        controlWrite(Message::Received, hdr.requestID, startupBuffer.data(), s.position());
    }

    // While we're here, we should also send the master schema to the client.


    // Log::info() << "Also, catalogue could now send store control endpoint to client: " << storeEndpoint << std::endl;
    // Log::info() << "Catalogue could also now send master schema " << std::endl;
    // Log::info() << " but... todo... " << std::endl;
}


// API forwarding logic, adapted from original remoteHandler
// Used for Inspect and List
// ***************************************************************************************
// All of the standard API functions behave in roughly the same manner. The Helper Classes
// described here capture the manner in which their behaviours differ.
//
// See forwardApiCall() for how these helpers are used to forward an API call using a
// worker thread.
// ***************************************************************************************
template <typename ValueType>
struct BaseHelper {
    virtual size_t encodeBufferSize(const ValueType&) const { return 4096; }
    void extraDecode(eckit::Stream&) {}
    ValueType apiCall(FDB&, const FDBToolRequest&) const { NOTIMP; }

    struct Encoded {
        size_t position;
        eckit::Buffer buf;
    };

    Encoded encode(const ValueType& elem, const CatalogueHandler&) const {
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
};

struct InspectHelper : public BaseHelper<ListElement> {
    ListIterator apiCall(FDB& fdb, const FDBToolRequest& request) const {
        return fdb.inspect(request.request());
    }
};

template <typename HelperClass>
void CatalogueHandler::forwardApiCall(const MessageHeader& hdr) {
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

void CatalogueHandler::handle() {
    initialiseConnections();

    Log::info() << "CatalogueServer started ..." << std::endl;

    MessageHeader hdr;
    eckit::FixedString<4> tail;

    // listen loop
    while (true) {
        tidyWorkers();

        Log::debug<LibFdb5>() << "CatalogueHandler::handle() - pre READ" << std::endl;
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
                    list(hdr);
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
                    inspect(hdr);
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

                case Message::Schema:
                    schema(hdr);
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

void CatalogueHandler::index(const MessageHeader& hdr) {
    NOTIMP;
}

void CatalogueHandler::read(const MessageHeader& hdr) {
    NOTIMP;
}

void CatalogueHandler::flush(const MessageHeader& hdr) {
    Buffer payload(receivePayload(hdr, controlSocket_));
    MemoryStream s(payload);

    size_t numArchived;
    s >> numArchived;

    ASSERT(numArchived == 0 || archiveFuture_.valid());

    if (archiveFuture_.valid()) {
        // Ensure that the expected number of fields have been written, and that the
        // archive worker processes have been cleanly wound up.
        size_t n = archiveFuture_.get();
        ASSERT(numArchived == n); // XXX Currently this will fail if there is more than one database in request.

        // Do the actual flush!
        Log::info() << "Flushing" << std::endl;
        Log::status() << "Flushing" << std::endl;

        for (auto it = catalogues_.begin(); it != catalogues_.end(); it++) {
            it->second->flush();
        }
    }
    Log::info() << "Flush complete" << std::endl;
    Log::status() << "Flush complete" << std::endl;
}

void CatalogueHandler::archive(const MessageHeader& hdr) {
    // NOTE identical to RemoteCatalogueWriter::archive()

    if(!archiveFuture_.valid()) {
        Log::debug<LibFdb5>() << "WIP CatalogueHandler::archive start threadloop" << std::endl;
        // Start archive worker thread
        archiveFuture_ = std::async(std::launch::async, [this] { return archiveThreadLoop(); });
    }
}

size_t CatalogueHandler::archiveThreadLoop() {
    size_t totalArchived = 0;

    // Create a worker that will do the actual archiving

    static size_t queueSize(eckit::Resource<size_t>("fdbServerMaxQueueSize", 32));
    // using ArchiveQueue = eckit::Queue<std::pair<uint32_t, std::pair<Key, std::unique_ptr<FieldLocation>>>>;
    // ArchiveQueue queue(queueSize);
    eckit::Queue<std::pair<uint32_t, eckit::Buffer>> queue(queueSize);


    std::future<size_t> worker = std::async(std::launch::async, [this, &queue] {
        size_t totalArchived = 0;
        std::pair<uint32_t, eckit::Buffer> elem = std::make_pair(0, Buffer{0});

        try {
            long queuelen;
            while ((queuelen = queue.pop(elem)) != -1) {
                uint32_t id = elem.first;
                // Key key = elem.second.first;
                eckit::Buffer payload = std::move(elem.second);
                MemoryStream s(payload);
                Key dbKey(s);
                Key idxKey(s);
                InspectionKey key; // xxx no stream constructor for inspection key?
                s >> key;
                std::unique_ptr<FieldLocation> location(eckit::Reanimator<FieldLocation>::reanimate(s));
                Log::debug<LibFdb5>() << "CatalogueHandler::archiveThreadLoop message has dbKey: " << dbKey 
                    << " idxKey: " << idxKey << " key: " << key << " and location.uri" << location->uri() << std::endl;
        
                CatalogueWriter& cat = catalogue(dbKey);
                cat.selectIndex(idxKey);
                cat.archive(key, std::move(location)); /// xxx currently this is making too many indexes.
                totalArchived += 1;
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
            Log::debug<LibFdb5>() << "CatalogueHandler::archiveThreadLoop awaiting message from client..." << std::endl;
            socketRead(&hdr, sizeof(hdr), dataSocket_);
            Log::debug<LibFdb5>() << "CatalogueHandler::archiveThreadLoop got a message from client!" << std::endl;

            ASSERT(hdr.marker == StartMarker);
            ASSERT(hdr.version == CurrentVersion);
            //ASSERT(hdr.requestID == id);

            // Have we been told that we are done yet?
            if (hdr.message == Message::Flush)
                break;

            ASSERT(hdr.message == Message::Blob); // no multiblobs for catalogue

            Buffer payload(receivePayload(hdr, dataSocket_));

            eckit::FixedString<4> tail;
            socketRead(&tail, sizeof(tail), dataSocket_);
            ASSERT(tail == EndMarker);

            size_t queuelen = queue.emplace(
                std::make_pair(hdr.requestID, std::move(payload))
            );
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
        dataWrite(Message::Error, 0, what.c_str(), what.length());
        queue.interrupt(std::current_exception());
        throw;
    }
    catch (...) {
        std::string what("Caught unexpected, unknown exception in retrieve worker");
        dataWrite(Message::Error, 0, what.c_str(), what.length());
        queue.interrupt(std::current_exception());
        throw;
    }

    return totalArchived;
}

void CatalogueHandler::list(const MessageHeader& hdr) {
    forwardApiCall<ListHelper>(hdr);
}

void CatalogueHandler::inspect(const MessageHeader& hdr) {
    // forwardApiCall<InspectHelper>(hdr);
    NOTIMP;
}

void CatalogueHandler::schema(const MessageHeader& hdr) {

    // 1. Read dbkey to select catalogue
    Buffer payload(receivePayload(hdr, controlSocket_));
    MemoryStream s(payload);
    Key dbKey(s);

    // 2. Get catalogue
    Catalogue& cat = catalogue(dbKey);
    const Schema& schema = cat.schema();
    eckit::Buffer schemaBuffer(1024*1024);
    eckit::MemoryStream stream(schemaBuffer);
    stream << schema;

    // todo: maybe more appropriate control write instead.
    dataWrite(Message::Schema, hdr.requestID, schemaBuffer, stream.position());
}

CatalogueWriter& CatalogueHandler::catalogue(Key dbKey) {
    auto it = catalogues_.find(dbKey);
    if (it != catalogues_.end()) {
        return *(it->second);
    }

    // xxx specifically catalogue writer right now.
    return *(catalogues_[dbKey] = CatalogueWriterFactory::instance().build(dbKey, config_));
}

}  // namespace fdb5::remote