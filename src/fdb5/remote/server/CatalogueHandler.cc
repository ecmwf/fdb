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
#include "eckit/net/NetMask.h"
#include "eckit/serialisation/MemoryStream.h"

#include "fdb5/LibFdb5.h"
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
    ServerConnection(socket, config) {}

CatalogueHandler::~CatalogueHandler() {}

Handled CatalogueHandler::handleControl(Message message, uint32_t clientID, uint32_t requestID) {

    try {
        switch (message) {
            case Message::Schema: // request top-level schema
                schema(clientID, requestID, eckit::Buffer(0));
                return Handled::Replied;

            case Message::Stores: // request the list of FDB stores and the corresponging endpoints
                stores(clientID, requestID);
                return Handled::Replied;


            case Message::Archive: // notification that the client is starting to send data locations for archival
                archiver();
                return Handled::YesAddArchiveListener;

            case Message::Flush: // notification that the client has sent all data locations for archival
                flush(clientID, requestID, eckit::Buffer{0});
                return Handled::Yes;

            default: {
                std::stringstream ss;
                ss << "ERROR: Unexpected message recieved (" << message << "). ABORTING";
                Log::status() << ss.str() << std::endl;
                Log::error() << ss.str() << std::endl;
                throw SeriousBug(ss.str(), Here());
            }
        }
    }
    catch (std::exception& e) {
        // n.b. more general than eckit::Exception
        error(e.what(), clientID, requestID);
    }
    catch (...) {
        error("Caught unexpected and unknown error", clientID, requestID);
    }
    return Handled::No;
}

Handled CatalogueHandler::handleControl(Message message, uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {

    try {
        switch (message) {

            case Message::Schema: // request catalogue schema
                schema(clientID, requestID, std::move(payload));
                return Handled::Replied;

            case Message::List: // list request. Location are sent aynchronously over the data connection
                list(clientID, requestID, std::move(payload));
                return Handled::Yes;

            case Message::Inspect: // inspect request. Location are sent aynchronously over the data connection
                inspect(clientID, requestID, std::move(payload));
                return Handled::Yes;

            case Message::Flush: // flush catalogue
                flush(clientID, requestID, std::move(payload));
                return Handled::Yes;

            default: {
                std::stringstream ss;
                ss << "ERROR: Unexpected message recieved (" << message << "). ABORTING";
                Log::status() << ss.str() << std::endl;
                Log::error() << ss.str() << std::endl;
                throw SeriousBug(ss.str(), Here());
            }
        }
    }
    catch (std::exception& e) {
        // n.b. more general than eckit::Exception
        error(e.what(), clientID, requestID);
    }
    catch (...) {
        error("Caught unexpected and unknown error", clientID, requestID);
    }
    return Handled::No;
}

// Handled CatalogueHandler::handleData(Message message, uint32_t clientID, uint32_t requestID) {
//     try {
//         switch (message) {
//             case Message::Flush: // notification that the client has sent all data locations for archival
//                 return Handled::YesRemoveArchiveListener; // ????
//
//             default: {
//                 std::stringstream ss;
//                 ss << "ERROR: Unexpected message recieved (" << message << "). ABORTING";
//                 Log::status() << ss.str() << std::endl;
//                 Log::error() << ss.str() << std::endl;
//                 throw SeriousBug(ss.str(), Here());
//             }
//         }
//     }
//     catch (std::exception& e) {
//         // n.b. more general than eckit::Exception
//         error(e.what(), clientID, requestID);
//     }
//     catch (...) {
//         error("Caught unexpected and unknown error", clientID, requestID);
//     }
//     return Handled::No;
// }

// Handled CatalogueHandler::handleData(Message message, uint32_t clientID, uint32_t requestID, /*eckit::net::Endpoint endpoint,*/ eckit::Buffer&& payload) {
//     try {
//         switch (message) {
//             case Message::Blob:
//             case Message::MultiBlob:
//                 queue(message, clientID, requestID, std::move(payload));
//                 return Handled::Yes;
//
//             default: {
//                 std::stringstream ss;
//                 ss << "ERROR: Unexpected message recieved (" << message << "). ABORTING";
//                 Log::status() << ss.str() << std::endl;
//                 Log::error() << ss.str() << std::endl;
//                 throw SeriousBug(ss.str(), Here());
//             }
//         }
//     }
//     catch (std::exception& e) {
//         // n.b. more general than eckit::Exception
//         error(e.what(), clientID, requestID);
//     }
//     catch (...) {
//         error("Caught unexpected and unknown error", clientID, requestID);
//     }
//     return Handled::No;
// }

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
void CatalogueHandler::forwardApiCall(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {
    HelperClass helper;

    MemoryStream s(payload);

    FDBToolRequest request(s);
    helper.extraDecode(s);

    // Construct worker thread to feed responses back to client

    ASSERT(workerThreads_.find(requestID) == workerThreads_.end());

    workerThreads_.emplace(
        requestID, std::async(std::launch::async, [request, clientID, requestID, helper, this]() {

            try {
                auto iterator = helper.apiCall(fdb_, request);
                typename decltype(iterator)::value_type elem;
                while (iterator.next(elem)) {
                    auto encoded(helper.encode(elem, *this));
                    write(Message::Blob, false, clientID, requestID, std::vector<std::pair<const void*, uint32_t>>{{encoded.buf, encoded.position}});
                }
                write(Message::Complete, false, clientID, requestID);
            }
            catch (std::exception& e) {
                // n.b. more general than eckit::Exception
                error(e.what(), clientID, requestID);
            }
            catch (...) {
                // We really don't want to std::terminate the thread
                error("Caught unexpected, unknown exception in worker", clientID, requestID);
            }
        }));
}

void CatalogueHandler::list(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {
    forwardApiCall<ListHelper>(clientID, requestID, std::move(payload));
}

void CatalogueHandler::inspect(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {
    forwardApiCall<InspectHelper>(clientID, requestID, std::move(payload));
}

void CatalogueHandler::schema(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {

    eckit::Buffer schemaBuffer(256*1024);
    eckit::MemoryStream stream(schemaBuffer);

    if (payload.size() == 0) { // client requesting the top-level schema
        stream << config_.schema();
    } else {
        // 1. Read dbkey to select catalogue
        MemoryStream s(payload);
        Key dbKey(s);
    
        // 2. Get catalogue
        Catalogue& cat = catalogue(clientID, dbKey);
        const Schema& schema = cat.schema();
        stream << schema;
    }

    write(Message::Received, false, clientID, requestID, schemaBuffer.data(), stream.position());
}

void CatalogueHandler::stores(uint32_t clientID, uint32_t requestID) {

    eckit::net::IPAddress clientIPaddress{controlSocket_.remoteAddr()};

    std::string clientNetwork = "";
    if (config_.has("networks")) {
        for (const auto& net: config_.getSubConfigurations("networks")) {
            if (net.has("name") && net.has("netmask")) {
                eckit::net::NetMask netmask{net.getString("netmask")};
                if (netmask.contains(clientIPaddress)) {
                    clientNetwork = net.getString("name");
                    break;
                }
            }
        }
    }

    Log::debug<LibFdb5>() << "Client " << clientIPaddress << " from network '" << clientNetwork << "'" << std::endl;

    ASSERT(config_.has("stores"));
    std::map<std::string, std::vector<eckit::net::Endpoint>> stores;
    // std::vector<std::pair<std::string, eckit::net::Endpoint>> stores;
    for (const auto& configStore: config_.getSubConfigurations("stores")) {
        ASSERT(configStore.has("default"));
        eckit::net::Endpoint fieldLocationEndpoint{configStore.getString("default")};
        eckit::net::Endpoint storeEndpoint{fieldLocationEndpoint};
        if (!clientNetwork.empty()) {
            if (configStore.has(clientNetwork)) {
                storeEndpoint = eckit::net::Endpoint{configStore.getString(clientNetwork)};
            }
        }

        auto it = stores.find(fieldLocationEndpoint);
        if (it == stores.end()) {
            stores.emplace(fieldLocationEndpoint, std::vector<eckit::net::Endpoint>{storeEndpoint});
        } else {
            it->second.push_back(storeEndpoint);
        }

        if (configStore.getBool("serveLocalData", false)) {
            it = stores.find("");
            if (it == stores.end()) {
                stores.emplace("", std::vector<eckit::net::Endpoint>{storeEndpoint});
            } else {
                it->second.push_back(storeEndpoint);
            }
        }
    }

    {
        Buffer startupBuffer(16*1024);
        MemoryStream s(startupBuffer);

        s << stores.size();
        for (const auto& store : stores) {
            s << store.first << store.second.size();
            for (const auto& ee: store.second) {
                s << ee;
            }
        }
        write(Message::Received, false, clientID, requestID, startupBuffer.data(), s.position());
    }
}







void CatalogueHandler::flush(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {
    
    size_t numArchived = 0;
    
    if (payload.size() > 0) {
        MemoryStream s(payload);
        s >> numArchived;
    }

    auto it = catalogues_.find(clientID);
    ASSERT(it != catalogues_.end());

    it->second.locationsExpected = numArchived;
    if (it->second.locationsArchived < numArchived) {
        it->second.archivalCompleted.get();
    }

    it->second.catalogue->flush();

    Log::info() << "Flush complete" << std::endl;
    Log::status() << "Flush complete" << std::endl;
}

// A helper function to make archiveThreadLoop a bit cleaner
void CatalogueHandler::archiveBlob(const uint32_t clientID, const uint32_t requestID, const void* data, size_t length) {

    MemoryStream s(data, length);

    // fdb5::Key dbKey(s);
    fdb5::Key idxKey(s);
    fdb5::InspectionKey key;
    s >> key; // xxx no stream constructor for inspection key?

    std::unique_ptr<FieldLocation> location(eckit::Reanimator<FieldLocation>::reanimate(s));

    Log::debug<LibFdb5>() << "CatalogueHandler::archiveBlob key: " << idxKey << key << "  location: " << location->uri() << std::endl;

    std::map<uint32_t, CatalogueArchiver>::iterator it;
    {
        std::lock_guard<std::mutex> lock(cataloguesMutex_);
        it = catalogues_.find(clientID);
        if (it == catalogues_.end()) {
            std::string what("Requested unknown catalogue id: " + std::to_string(clientID));
            error(what, 0, 0);
            throw;
        }
    }

    it->second.catalogue->selectIndex(idxKey);
    it->second.catalogue->archive(key, std::move(location));
    it->second.locationsArchived++;
    if (it->second.locationsExpected > 0 && it->second.locationsExpected == it->second.locationsArchived) {
        it->second.fieldLocationsReceived.set_value(it->second.locationsExpected);
    }
}

bool CatalogueHandler::remove(uint32_t clientID) {
    
    std::stringstream ss;
    ss << "remove " << clientID << " from " << catalogues_.size() << " clients - ids:";
    for (auto it = catalogues_.begin(); it != catalogues_.end(); it++)
        ss << "  " << it->first;
    ss << std::endl;
    eckit::Log::info() << ss.str();

    std::lock_guard<std::mutex> lock(cataloguesMutex_);

    auto it = catalogues_.find(clientID);
    if (it != catalogues_.end()) {
        catalogues_.erase(it);
    }

    return catalogues_.empty();
}


CatalogueWriter& CatalogueHandler::catalogue(uint32_t id, const Key& dbKey) {
    std::lock_guard<std::mutex> lock(cataloguesMutex_);
    return *((catalogues_.emplace(id, CatalogueArchiver(dbKey, config_)).first)->second.catalogue);
}

}  // namespace fdb5::remote