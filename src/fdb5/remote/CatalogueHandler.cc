/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/remote/CatalogueHandler.h"

#include "eckit/serialisation/MemoryStream.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/FDBFactory.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

using namespace eckit;
using metkit::mars::MarsRequest;

namespace fdb5::remote {

namespace {

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

template <typename ValueType>
struct BaseHelper {
    virtual size_t encodeBufferSize(const ValueType&) const { return 4096; }
    void extraDecode(eckit::Stream&) {}
    ValueType apiCall(FDBCatalogueBase&, const FDBToolRequest&) const { NOTIMP; }

    struct Encoded {
        size_t position;
        eckit::Buffer buf;
    };

    Encoded encode(const ValueType& elem, const DecoupledHandler&) const {
        eckit::Buffer encodeBuffer(encodeBufferSize(elem));
        MemoryStream s(encodeBuffer);
        s << elem;
        return {s.position(), std::move(encodeBuffer)};
    }
};

struct ListHelper : public BaseHelper<ListElement> {
    ListIterator apiCall(FDBCatalogueBase& fdb, const FDBToolRequest& request) const {
        return fdb.list(request);
    }
};

struct InspectHelper : public BaseHelper<ListElement> {
    ListIterator apiCall(FDBCatalogueBase& fdb, const FDBToolRequest& request) const {
        return fdb.inspect(request.request());
    }
};

}


CatalogueHandler::CatalogueHandler(eckit::net::TCPSocket& socket, const Config& config):
    DecoupledHandler(socket, config) {

}
CatalogueHandler::~CatalogueHandler() {}

void CatalogueHandler::initialiseConnections() {
    DecoupledHandler::initialiseConnections();

    std::string storeHost = ::getenv("FDB_STORE_HOST") ? ::getenv("FDB_STORE_HOST") : "localhost";
    std::string storePort = ::getenv("FDB_STORE_PORT") ? ::getenv("FDB_STORE_PORT") : "7000";
    net::Endpoint storeEndpoint(storeHost, std::stoi(storePort));


    // Log::info() << "Sending store endpoint to client: " << storeEndpoint << std::endl;
    // {
    //     Buffer startupBuffer(1024);
    //     MemoryStream s(startupBuffer);

    //     s << clientSession;
    //     s << sessionID_;
    //     s << storeEndpoint;

    //     // s << storeEndpoint; // xxx single-store case only: we cant do this with multiple stores // For now, dont send the store endpoint to the client 

    //     Log::debug() << "Protocol negotiation - configuration: " << agreedConf_ <<std::endl;


    //     controlWrite(Message::Startup, 0, startupBuffer.data(), s.position());
    // }


    Log::info() << "Also, catalogue could now send store control endpoint to client: " << storeEndpoint << std::endl;
    Log::info() << " but... todo... " << std::endl;
}

void CatalogueHandler::handle() {
    initialiseConnections();

    Log::info() << "CatalogueServer started ..." << std::endl;

    MessageHeader hdr;
    eckit::FixedString<4> tail;

    // listen loop
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
                    forwardApiCall<InspectHelper>(hdr);
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

                // case Message::Store:
                //     store(hdr);
                //     break;

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


// API

template <typename HelperClass>
void CatalogueHandler::forwardApiCall(const MessageHeader& hdr) {
    HelperClass helper;

//    std::cout << "CatalogueHandler::forwardApiCall" << std::endl;

    Buffer payload(receivePayload(hdr, controlSocket_));
    MemoryStream s(payload);

    FDBToolRequest request(s);
    helper.extraDecode(s);

    // Construct worker thread to feed responses back to client

    ASSERT(catalogue_);
    ASSERT(workerThreads_.find(hdr.requestID) == workerThreads_.end());

    workerThreads_.emplace(
        hdr.requestID, std::async(std::launch::async, [request, hdr, helper, this]() {
            try {
                auto iterator = helper.apiCall(*catalogue_, request);

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

}  // namespace fdb5::remote
