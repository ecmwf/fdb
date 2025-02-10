/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/remote/server/CatalogueHandler.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Key.h"
#include "fdb5/remote/Connection.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/remote/server/ServerConnection.h"

#include "eckit/net/NetMask.h"
#include "eckit/net/TCPSocket.h"
#include "eckit/serialisation/MemoryStream.h"

#include <future>
#include <mutex>
#include <utility>

using namespace eckit;

namespace fdb5::remote {

// ***************************************************************************************
//
// Note that we use the "control" and "data" connections in a specific way, although these
// may not be the optimal names for them. "control" is used for blocking requests,
// and "data" is used for non-blocking activity.
//
// ***************************************************************************************

CatalogueHandler::CatalogueHandler(eckit::net::TCPSocket& socket, const Config& config) :
    ServerConnection(socket, config), fdbControlConnection_(false), fdbDataConnection_(false) {}

CatalogueHandler::~CatalogueHandler() {}

Handled CatalogueHandler::handleControl(Message message, uint32_t clientID, uint32_t requestID) {

    try {
        switch (message) {
            case Message::Schema:  // request top-level schema
            {
                std::lock_guard<std::mutex> lock(handlerMutex_);
                auto it = fdbs_.find(clientID);
                if (it == fdbs_.end()) {
                    fdbs_[clientID];
                    fdbControlConnection_ = true;
                    fdbDataConnection_    = !single_;
                    numControlConnection_++;
                    if (fdbDataConnection_)
                        numDataConnection_++;
                }
            }
                schema(clientID, requestID, eckit::Buffer(0));
                return Handled::Replied;

            case Message::Stores:  // request the list of FDB stores and the corresponging endpoints
                stores(clientID, requestID);
                return Handled::Replied;


            case Message::Archive:  // notification that the client is starting to send data locations for archival
                archiver();
                return Handled::YesAddArchiveListener;

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

Handled CatalogueHandler::handleControl(Message message, uint32_t clientID, uint32_t requestID,
                                        eckit::Buffer&& payload) {

    try {
        switch (message) {

            case Message::Schema:  // request catalogue schema
                schema(clientID, requestID, std::move(payload));
                return Handled::Replied;

            case Message::List:  // list request. Location are sent aynchronously over the data connection
                list(clientID, requestID, std::move(payload));
                return Handled::Yes;

            case Message::Axes:  // list request. Location are sent aynchronously over the data connection
                axes(clientID, requestID, std::move(payload));
                return Handled::Yes;

            case Message::Inspect:  // inspect request. Location are sent aynchronously over the data connection
                inspect(clientID, requestID, std::move(payload));
                return Handled::Yes;

            case Message::Stats:  // inspect request. Location are sent aynchronously over the data connection
                stats(clientID, requestID, std::move(payload));
                return Handled::Yes;

            case Message::Flush:  // flush catalogue
                flush(clientID, requestID, std::move(payload));
                return Handled::Yes;

            case Message::Exists:  // check if catalogue exists
                exists(clientID, requestID, std::move(payload));
                return Handled::Replied;

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
        /// @todo remember to add level_ to this helper
        return fdb.list(request);
    }
};

struct AxesHelper : public BaseHelper<AxesElement> {
    virtual size_t encodeBufferSize(const AxesElement& el) const { return el.encodeSize(); }

    void extraDecode(eckit::Stream& s) { s >> level_; }
    AxesIterator apiCall(FDB& fdb, const FDBToolRequest& request) const { return fdb.axesIterator(request, level_); }

private:

    int level_;
};

struct InspectHelper : public BaseHelper<ListElement> {
    ListIterator apiCall(FDB& fdb, const FDBToolRequest& request) const { return fdb.inspect(request.request()); }
};

struct StatsHelper : public BaseHelper<StatsElement> {
    StatsIterator apiCall(FDB& fdb, const FDBToolRequest& request) const { return fdb.stats(request); }
};

template <typename HelperClass>
void CatalogueHandler::forwardApiCall(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {
    HelperClass helper;

    MemoryStream s(payload);

    FDBToolRequest request(s);
    helper.extraDecode(s);

    // Construct worker thread to feed responses back to client
    ASSERT(workerThreads_.find(requestID) == workerThreads_.end());

    {
        std::lock_guard<std::mutex> lock(fdbMutex_);
        auto it = fdbs_.find(clientID);
        if (it == fdbs_.end()) {
            fdbs_[clientID];
        }
    }

    workerThreads_.emplace(requestID, std::async(std::launch::async, [request, clientID, requestID, helper, this]() {
                               try {
                                   FDB* fdb = nullptr;
                                   {
                                       std::lock_guard<std::mutex> lock(handlerMutex_);
                                       auto it = fdbs_.find(clientID);
                                       ASSERT(it != fdbs_.end());
                                       fdb = &it->second;
                                       ASSERT(fdb);
                                   }
                                   auto iterator = helper.apiCall(*fdb, request);
                                   typename decltype(iterator)::value_type elem;
                                   while (iterator.next(elem)) {
                                       auto encoded(helper.encode(elem, *this));
                                       write(Message::Blob, false, clientID, requestID, encoded.buf, encoded.position);
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

void CatalogueHandler::axes(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {
    forwardApiCall<AxesHelper>(clientID, requestID, std::move(payload));
}

void CatalogueHandler::inspect(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {
    forwardApiCall<InspectHelper>(clientID, requestID, std::move(payload));
}

void CatalogueHandler::stats(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {
    forwardApiCall<StatsHelper>(clientID, requestID, std::move(payload));
}

void CatalogueHandler::schema(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {

    eckit::Buffer schemaBuffer(256 * 1024);
    eckit::MemoryStream stream(schemaBuffer);

    if (payload.size() == 0) {  // client requesting the top-level schema
        stream << config_.schema();
    }
    else {
        // 1. Read dbkey to select catalogue
        MemoryStream s(payload);
        Key dbKey(s);

        // 2. Get catalogue
        Catalogue& cat       = catalogue(clientID, dbKey);
        const Schema& schema = cat.schema();
        stream << schema;
    }

    write(Message::Received, true, clientID, requestID, schemaBuffer.data(), stream.position());
}

void CatalogueHandler::stores(uint32_t clientID, uint32_t requestID) {

    eckit::net::IPAddress clientIPaddress{controlSocket_.remoteAddr()};

    std::string clientNetwork = "";
    if (config_.has("networks")) {
        for (const auto& net : config_.getSubConfigurations("networks")) {
            if (net.has("name") && net.has("netmask")) {
                eckit::net::NetMask netmask{net.getString("netmask")};
                if (netmask.contains(clientIPaddress)) {
                    clientNetwork = net.getString("name");
                    break;
                }
            }
        }
    }

    LOG_DEBUG_LIB(LibFdb5) << "Client " << clientIPaddress << " from network '" << clientNetwork << "'" << std::endl;

    ASSERT(config_.has("stores"));
    std::map<std::string, std::vector<eckit::net::Endpoint>> stores;
    for (const auto& configStore : config_.getSubConfigurations("stores")) {
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
        }
        else {
            it->second.push_back(storeEndpoint);
        }

        if (configStore.getBool("serveLocalData", false)) {
            it = stores.find("");
            if (it == stores.end()) {
                stores.emplace("", std::vector<eckit::net::Endpoint>{storeEndpoint});
            }
            else {
                it->second.push_back(storeEndpoint);
            }
        }
    }

    {
        Buffer startupBuffer(16 * 1024);
        MemoryStream s(startupBuffer);

        s << stores.size();
        for (const auto& store : stores) {
            s << store.first << store.second.size();
            for (const auto& ee : store.second) {
                s << ee;
            }
        }
        write(Message::Received, true, clientID, requestID, startupBuffer.data(), s.position());
    }
}

void CatalogueHandler::exists(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) const {

    ASSERT(payload.size() > 0);

    bool exists = false;

    {
        eckit::MemoryStream stream(payload);
        const Key dbKey(stream);
        exists = CatalogueReaderFactory::instance().build(dbKey, config_)->exists();
    }

    eckit::Buffer existBuf(5);
    eckit::MemoryStream stream(existBuf);
    stream << exists;

    write(Message::Received, true, clientID, requestID, existBuf.data(), stream.position());
}

void CatalogueHandler::flush(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {

    ASSERT(payload.size() > 0);

    size_t numArchived = 0;
    MemoryStream s(payload);
    s >> numArchived;

    if (numArchived == 0) {
        return;
    }

    auto it = catalogues_.find(clientID);
    ASSERT(it != catalogues_.end());

    {
        std::lock_guard<std::mutex> lock(fieldLocationsMutex_);
        it->second.locationsExpected =
            numArchived;  // setting locationsExpected also means that a flush has been requested
        it->second.archivalCompleted = it->second.fieldLocationsReceived.get_future();
        if (it->second.locationsArchived == numArchived) {
            it->second.fieldLocationsReceived.set_value(numArchived);
        }
    }

    it->second.archivalCompleted.wait();
    {
        std::lock_guard<std::mutex> lock(fieldLocationsMutex_);
        it->second.fieldLocationsReceived = std::promise<size_t>{};
        it->second.locationsExpected      = 0;
        it->second.locationsArchived      = 0;
    }

    it->second.catalogue->flush(numArchived);

    Log::info() << "Flush complete" << std::endl;
    Log::status() << "Flush complete" << std::endl;
}

// A helper function to make archiveThreadLoop a bit cleaner
void CatalogueHandler::archiveBlob(const uint32_t clientID, const uint32_t requestID, const void* data, size_t length) {

    MemoryStream s(data, length);

    fdb5::Key idxKey(s);
    fdb5::Key datumKey(s);

    std::unique_ptr<FieldLocation> location(eckit::Reanimator<FieldLocation>::reanimate(s));

    LOG_DEBUG_LIB(LibFdb5) << "CatalogueHandler::archiveBlob key: " << idxKey << datumKey
                           << "  location: " << location->uri() << std::endl;

    std::map<uint32_t, CatalogueArchiver>::iterator it;
    {
        std::lock_guard<std::mutex> lock(handlerMutex_);
        it = catalogues_.find(clientID);
        if (it == catalogues_.end()) {
            std::string what("Requested unknown catalogue id: " + std::to_string(clientID));
            error(what, 0, 0);
            throw;
        }
    }

    it->second.catalogue->selectIndex(idxKey);
    it->second.catalogue->archive(idxKey, datumKey, std::move(location));
    {
        std::lock_guard<std::mutex> lock(fieldLocationsMutex_);
        it->second.locationsArchived++;
        if (it->second.locationsExpected != 0 && it->second.archivalCompleted.valid() &&
            it->second.locationsExpected == it->second.locationsArchived) {
            it->second.fieldLocationsReceived.set_value(it->second.locationsExpected);
        }
    }
}

bool CatalogueHandler::remove(bool control, uint32_t clientID) {

    std::lock_guard<std::mutex> lock(handlerMutex_);

    // is the client an FDB
    auto it = fdbs_.find(clientID);
    if (it != fdbs_.end()) {
        if (control) {
            fdbControlConnection_ = false;
            numControlConnection_--;
        }
        else {
            fdbDataConnection_ = false;
            numDataConnection_--;
        }
    }
    else {

        auto it = catalogues_.find(clientID);
        if (it != catalogues_.end()) {
            if (control) {
                it->second.controlConnection = false;
                numControlConnection_--;
            }
            else {
                it->second.dataConnection = false;
                numDataConnection_--;
            }
            if (!it->second.controlConnection && !it->second.dataConnection) {
                catalogues_.erase(it);
            }
        }
    }
    return ((control ? numControlConnection_ : numDataConnection_) == 0);
}

CatalogueWriter& CatalogueHandler::catalogue(uint32_t id, const Key& dbKey) {
    std::lock_guard<std::mutex> lock(handlerMutex_);
    numControlConnection_++;
    if (!single_) {
        numDataConnection_++;
    }
    return *((catalogues_.emplace(id, CatalogueArchiver(!single_, dbKey, config_)).first)->second.catalogue);
}

}  // namespace fdb5::remote
