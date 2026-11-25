// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/remote/server/CatalogueHandler.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/FDBFactory.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/WipeState.h"
#include "fdb5/remote/Connection.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/remote/server/ServerConnection.h"
#include "fdb5/tracing/TraceContext.h"
#include "fdb5/tracing/Tracer.h"

#include "eckit/config/Resource.h"
#include "eckit/net/TCPSocket.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/serialisation/ResizableMemoryStream.h"
#include "eckit/utils/Literals.h"

#include <chrono>
#include <future>
#include <mutex>
#include <optional>
#include <utility>

using namespace eckit;
using namespace eckit::literals;

namespace fdb5::remote {

// ***************************************************************************************
//
// Note that we use the "control" and "data" connections in a specific way, although these
// may not be the optimal names for them. "control" is used for blocking requests,
// and "data" is used for non-blocking activity.
//
// ***************************************************************************************

CatalogueHandler::CatalogueHandler(eckit::net::TCPSocket& socket, const Config& config) :
    ServerConnection(socket, config) {}

CatalogueHandler::~CatalogueHandler() {
    // the workers use the members of this class
    stopWorkers(true);
}

Handled CatalogueHandler::handleControl(Message message, uint32_t clientID, uint32_t requestID) {

    try {
        switch (message) {
            case Message::Schema:  // request top-level schema
            {
                std::lock_guard<std::mutex> lock(handlerMutex_);
                auto it = fdbs_.find(clientID);
                if (it == fdbs_.end()) {
                    fdbs_.emplace(clientID, std::make_shared<FDB>(innerConfig_));
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
                std::ostringstream ss;
                ss << "ERROR: Unexpected message recieved (" << message << "). ABORTING";
                Log::status() << ss.str() << std::endl;
                Log::error() << ss.str() << std::endl;
                throw SeriousBug(ss.str(), Here());
            }
        }
    }
    catch (std::exception& e) {
        // n.b. more general than eckit::Exception
        error(true, e.what(), clientID, requestID);
    }
    catch (...) {
        error(true, "Caught unexpected and unknown error", clientID, requestID);
    }
    return Handled::Replied;
}

Handled CatalogueHandler::handleControl(Message message, uint32_t clientID, uint32_t requestID,
                                        eckit::Buffer&& payload) {

    try {
        if (!enabled(agreedConf_.enabledFeatures(), message)) {
            std::ostringstream ss;
            // not agreed for this connection: disabled by either side, or not granted to the network of the client
            ss << "'" << message << "' is not permitted for this client on FDB server " << host() << ":" << port();
            unauthorised(ss.str(), clientID, requestID);
            return Handled::Replied;
        }

        switch (message) {

            case Message::Schema:  // request catalogue schema
                schema(clientID, requestID, std::move(payload));
                return Handled::Replied;

            case Message::List:  // list request. Location are sent aynchronously over the data connection
                list(clientID, requestID, std::move(payload));
                return Handled::Yes;

            case Message::Axes:  // axes request.
                axes(clientID, requestID, std::move(payload));
                return Handled::Yes;

            case Message::Inspect:  // inspect request. Location are sent aynchronously over the data connection
                inspect(clientID, requestID, std::move(payload));
                return Handled::Yes;

            case Message::Stats:  // inspect request. Location are sent aynchronously over the data connection
                stats(clientID, requestID, std::move(payload));
                return Handled::Yes;

            case Message::Status:  // database-level status, including access timestamps
                status(clientID, requestID, std::move(payload));
                return Handled::Yes;

            case Message::Touch:  // explicitly record a read and/or a write
                touch(clientID, requestID, std::move(payload));
                return Handled::Yes;

            case Message::Flush:  // flush catalogue
                flush(clientID, requestID, std::move(payload));
                return Handled::Yes;

            case Message::Exists:  // check if catalogue exists
                exists(clientID, requestID, std::move(payload));
                return Handled::Replied;

            case Message::Wipe:  // Initial wipe request
                wipe(clientID, requestID, std::move(payload));
                return Handled::Yes;

            case Message::DoMaskIndexEntries:
                // doit! We expect DoMaskIndexEntries, doWipeURIs, DoWipeUnknowns and doWipeEmptyDatabase in succession
                doMaskIndexEntries(clientID, requestID, std::move(payload));
                return Handled::Yes;

            case Message::DoWipeURIs:  // Do the wipe on our currentWipeState
                doWipeURIs(clientID, requestID, std::move(payload));
                return Handled::Yes;

            case Message::DoWipeFinish:  // Finish wipe by deleting empty DBs
                doWipeEmptyDatabase(clientID, requestID, std::move(payload));
                return Handled::Yes;

            case Message::DoWipeUnknowns:  // Wipe a set of unknown URIs
                doWipeUnknowns(clientID, requestID, std::move(payload));
                return Handled::Yes;

            case Message::DoUnsafeFullWipe:  // wipe a full database including its content
                doUnsafeFullWipe(clientID, requestID, std::move(payload));
                return Handled::Replied;

            default: {
                std::ostringstream ss;
                ss << "ERROR: Unexpected message recieved (" << message << "). ABORTING";
                Log::status() << ss.str() << std::endl;
                Log::error() << ss.str() << std::endl;
                throw SeriousBug(ss.str(), Here());
            }
        }
    }
    catch (std::exception& e) {
        // n.b. more general than eckit::Exception
        error(true, e.what(), clientID, requestID);
    }
    catch (...) {
        error(true, "Caught unexpected and unknown error", clientID, requestID);
    }
    return Handled::Replied;
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
    ValueType makeValue(const CatalogueHandler&) const { return ValueType(); }

    struct Encoded {
        size_t position;
        eckit::Buffer buf;
    };

    Encoded encode(const ValueType& elem, CatalogueHandler&) const {
        eckit::Buffer encodeBuffer(encodeBufferSize(elem));
        MemoryStream s(encodeBuffer);
        s << elem;
        return {s.position(), std::move(encodeBuffer)};
    }
};

struct ListHelper : public BaseHelper<ListElement> {
    ListIterator apiCall(FDB& fdb, const FDBToolRequest& request) const { return fdb.list(request, false, depth_); }

    void extraDecode(eckit::Stream& s) { s >> depth_; }

private:

    int depth_{3};
};

struct AxesHelper : public BaseHelper<AxesElement> {
    virtual size_t encodeBufferSize(const AxesElement& el) const { return el.encodeSize(); }

    void extraDecode(eckit::Stream& s) { s >> level_; }
    AxesIterator apiCall(FDB& fdb, const FDBToolRequest& request) const { return fdb.axesIterator(request, level_); }

private:

    int level_;
};

struct WipeHelper : public BaseHelper<CatalogueWipeState> {

    /// @note: We always use a resizeable memory stream, this just sets an initial size.
    virtual size_t encodeBufferSize(const CatalogueWipeState& el) const { return 1_MiB; }

    Encoded encode(const CatalogueWipeState& state, CatalogueHandler& handler) const {
        eckit::Buffer encodeBuffer(encodeBufferSize(state));
        ResizableMemoryStream s(encodeBuffer);

        const std::string dummy_secret = eckit::Resource<std::string>("$FDB_WIPE_SECRET;fdbWipeSecret", "");
        ASSERT(!dummy_secret.empty());

        state.signStoreStates(dummy_secret);

        s << state;

        const Key& dbKey = state.dbKey();
        std::lock_guard lock(handler.wipesMutex_);

        if (doit_) {
            // Keep a local copy of the catalogue wipe state, awaiting an explicit doWipeURIs command from the client

            // Expect this dbKey not to already be in progress
            ASSERT(handler.wipesInProgress_.find(dbKey) == handler.wipesInProgress_.end());

            handler.wipesInProgress_.emplace(
                dbKey, CatalogueHandler::WipeInProgress{
                           unsafeWipeAll_, CatalogueReaderFactory::instance().build(dbKey, state.config()),
                           CatalogueWipeState(dbKey, state.safeURIs(), state.deleteMap(), state.indexesToMask(),
                                              state.config(), state.unrecognisedURIs())});
        }
        else {
            handler.wipesInProgress_.erase(dbKey);
        }
        return {s.position(), std::move(encodeBuffer)};
    }

    void extraDecode(eckit::Stream& s) {
        s >> doit_;
        s >> unsafeWipeAll_;
    }

    WipeStateIterator apiCall(FDB& fdb, const FDBToolRequest& request) const {
        return fdb.internal_->wipe(request, doit_, false, unsafeWipeAll_);
    }

    CatalogueWipeState makeValue(const CatalogueHandler& handler) const {
        return CatalogueWipeState(Key(), handler.config());
    }

private:

    bool doit_;
    bool unsafeWipeAll_;
};

struct InspectHelper : public BaseHelper<ListElement> {
    ListIterator apiCall(FDB& fdb, const FDBToolRequest& request) const { return fdb.inspect(request.request()); }
};

struct StatsHelper : public BaseHelper<StatsElement> {
    StatsIterator apiCall(FDB& fdb, const FDBToolRequest& request) const { return fdb.stats(request); }
};

struct StatusHelper : public BaseHelper<StatusElement> {
    StatusIterator apiCall(FDB& fdb, const FDBToolRequest& request) const { return fdb.status(request); }
};

struct TouchHelper : public BaseHelper<StatusElement> {

    void extraDecode(eckit::Stream& s) {
        identifiers_ = AccessIdentifiers(s);
        when_ = AccessTime(s);
    }

    StatusIterator apiCall(FDB& fdb, const FDBToolRequest& request) const {
        return fdb.touch(request, identifiers_, when_);
    }

private:

    AccessIdentifiers identifiers_;
    AccessTime when_;
};

template <typename HelperClass>
void CatalogueHandler::handleApiCall(const char* spanName, uint32_t clientID, uint32_t requestID,
                                     eckit::Buffer&& payload) {
    HelperClass helper;

    MemoryStream s(payload);

    FDBToolRequest request(s);
    helper.extraDecode(s);

    // Construct worker thread to feed responses back to client
    ASSERT(workerThreads_.find(requestID) == workerThreads_.end());

    // pinned by the worker: it stays alive even if the client is removed meanwhile
    std::shared_ptr<FDB> clientFDB = fdb(clientID);

    // The worker streams the results back: it is traced as a server span, child of the client's request
    workerThreads_.emplace(
        requestID, std::async(std::launch::async, [request, clientID, requestID, helper, clientFDB, this, spanName,
                                                   traceContext = tracing::current()]() {
            std::optional<tracing::ScopedSpan> span;
            if (tracing_) {
                span.emplace(spanName, tracing::SpanKind::Server, traceContext);
            }
            try {
                auto iterator = helper.apiCall(*clientFDB, request);

                typename decltype(iterator)::value_type elem = helper.makeValue(*this);
                while (iterator.next(elem)) {
                    auto encoded(helper.encode(elem, *this));
                    write(Message::Blob, false, clientID, requestID, encoded.buf, encoded.position);
                }
                write(Message::Complete, false, clientID, requestID);
            }
            catch (std::exception& e) {
                // n.b. more general than eckit::Exception
                if (span) {
                    span->span().setStatus(tracing::StatusCode::Error, e.what());
                }
                tryError(false, e.what(), clientID, requestID);
            }
            catch (...) {
                // We really don't want to std::terminate the thread
                if (span) {
                    span->span().setStatus(tracing::StatusCode::Error, "unknown exception");
                }
                tryError(false, "Caught unexpected, unknown exception in worker", clientID, requestID);
            }
        }));
}

const char* CatalogueHandler::serverSpanName(Message message) const {
    // the initial wipe request is an asynchronous API call, traced by its worker
    return message == Message::Wipe ? nullptr : ServerConnection::serverSpanName(message);
}

void CatalogueHandler::list(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {
    handleApiCall<ListHelper>("fdb.server.list", clientID, requestID, std::move(payload));
}

void CatalogueHandler::axes(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {
    handleApiCall<AxesHelper>("fdb.server.axes", clientID, requestID, std::move(payload));
}

void CatalogueHandler::wipe(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {
    handleApiCall<WipeHelper>("fdb.server.wipe", clientID, requestID, std::move(payload));
}

void CatalogueHandler::inspect(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {
    handleApiCall<InspectHelper>("fdb.server.inspect", clientID, requestID, std::move(payload));
}

void CatalogueHandler::stats(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {
    handleApiCall<StatsHelper>("fdb.server.stats", clientID, requestID, std::move(payload));
}

void CatalogueHandler::status(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {
    handleApiCall<StatusHelper>(clientID, requestID, std::move(payload));
}

void CatalogueHandler::touch(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {
    handleApiCall<TouchHelper>(clientID, requestID, std::move(payload));
}

void CatalogueHandler::schema(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {

    eckit::Buffer schemaBuffer(256_KiB);
    eckit::MemoryStream stream(schemaBuffer);

    if (payload.size() == 0) {  // client requesting the top-level schema
        stream << innerConfig_.schema();
    }
    else {
        // 1. Read dbkey to select catalogue
        MemoryStream s(payload);
        Key dbKey(s);

        // 2. Get catalogue
        Catalogue& cat = catalogue(clientID, dbKey);
        const Schema& schema = cat.schema();
        stream << schema;
    }

    write(Message::Received, true, clientID, requestID, schemaBuffer.data(), stream.position());
}

void CatalogueHandler::stores(uint32_t clientID, uint32_t requestID) {

    ASSERT(config_.has("stores"));
    std::map<std::string, std::vector<eckit::net::Endpoint>> stores;
    for (const auto& configStore : config_.getSubConfigurations("stores")) {
        ASSERT(configStore.has("default"));
        eckit::net::Endpoint fieldLocationEndpoint{configStore.getString("default")};
        eckit::net::Endpoint storeEndpoint{fieldLocationEndpoint};
        if (!clientNetwork_.empty()) {
            if (configStore.has(clientNetwork_)) {
                storeEndpoint = eckit::net::Endpoint{configStore.getString(clientNetwork_)};
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
        Buffer startupBuffer(16_KiB);
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
        exists = CatalogueReaderFactory::instance().build(dbKey, innerConfig_)->exists();
    }

    eckit::Buffer existBuf(5);
    eckit::MemoryStream stream(existBuf);
    stream << exists;

    write(Message::Received, true, clientID, requestID, existBuf.data(), stream.position());
}

void CatalogueHandler::flush(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {

    // The archive spans of the client end within the flush span (current while handling this message), also if the
    // flush fails: then they are exported with it (see keep_errors)
    struct EndArchiveSpans {
        tracing::SpanAggregator& spans;
        uint32_t clientID;
        ~EndArchiveSpans() { spans.end(clientID); }
    } endArchiveSpans{archiveSpans_, clientID};

    ASSERT(payload.size() > 0);

    size_t numArchived = 0;
    MemoryStream s(payload);
    s >> numArchived;

    if (numArchived == 0) {
        return;
    }

    rethrowArchiveFailure();

    auto archiver = catalogueArchiver(clientID);
    if (!archiver) {
        throw SeriousBug("Flush requested for unknown catalogue id: " + std::to_string(clientID), Here());
    }
    // n.b. pinned while the flush waits

    {
        std::lock_guard<std::mutex> lock(fieldLocationsMutex_);
        archiver->locationsExpected =
            numArchived;  // setting locationsExpected also means that a flush has been requested
        archiver->archivalCompleted = archiver->fieldLocationsReceived.get_future();
        if (archiver->locationsArchived == numArchived) {
            archiver->fieldLocationsReceived.set_value(numArchived);
        }
    }

    while (archiver->archivalCompleted.wait_for(std::chrono::milliseconds(100)) != std::future_status::ready) {
        rethrowArchiveFailure();

        if (aborted()) {
            throw SeriousBug("Connection terminated before all field locations were archived", Here());
        }

        // The archive worker only finishes before teardown if it failed; don't wait for locations that will never come
        if (archiveFuture_.valid() && archiveFuture_.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
            static_cast<void>(archiveFuture_.get());  // rethrows the failure of the worker, if any
            throw SeriousBug("Archive worker terminated before all field locations were archived", Here());
        }

        // No more locations can arrive (single connection, or the data connection has ended), and those already
        // queued have been archived: a count still short will never complete
        if (archiveDataEnded() && archivePending() == 0 &&
            archiver->archivalCompleted.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
            // the failure of the last element processed, if any, is published before it was counted as processed:
            // it is the reason of the short count
            rethrowArchiveFailure();

            size_t archived = 0;
            {
                std::lock_guard<std::mutex> lock(fieldLocationsMutex_);
                archived = archiver->locationsArchived;
            }
            std::ostringstream ss;
            ss << "Flush of " << numArchived << " field locations, but only " << archived
               << " were received: no more can arrive";
            throw SeriousBug(ss.str(), Here());
        }
    }
    {
        std::lock_guard<std::mutex> lock(fieldLocationsMutex_);
        archiver->fieldLocationsReceived = std::promise<size_t>{};
        archiver->locationsExpected = 0;
        archiver->locationsArchived = 0;
    }

    archiver->catalogue->flush(numArchived);

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

    // pinned: a Stop may remove the client meanwhile
    std::shared_ptr<CatalogueArchiver> archiver;
    {
        std::lock_guard<std::mutex> lock(handlerMutex_);
        auto it = catalogues_.find(clientID);
        if (it == catalogues_.end()) {
            if (removedClient(clientID)) {
                // the client went away without flushing: nobody is waiting for this location any more
                Log::warning() << "Discarding a field location for the removed client " << clientID << std::endl;
                return;
            }
            throw SeriousBug("Requested unknown catalogue id: " + std::to_string(clientID), Here());
        }
        archiver = it->second;
    }

    if (!archiver->catalogue->selectIndex(idxKey)) {
        archiver->catalogue->createIndex(idxKey, datumKey.keys().size());
    }
    archiver->catalogue->archive(idxKey, datumKey, std::move(location));
    {
        std::lock_guard<std::mutex> lock(fieldLocationsMutex_);
        archiver->locationsArchived++;
        if (archiver->locationsExpected != 0 && archiver->archivalCompleted.valid() &&
            archiver->locationsExpected == archiver->locationsArchived) {
            archiver->fieldLocationsReceived.set_value(archiver->locationsExpected);
        }
    }
}

void CatalogueHandler::remove(uint32_t clientID) {

    std::lock_guard<std::mutex> lock(handlerMutex_);

    // The client is gone (Stop): its state is released as soon as nobody uses it any more (see catalogueArchiver(),
    // fdb()). It is marked as removed whether or not it has a state yet: what is still queued for it is discarded.
    removedClients_.insert(clientID);
    fdbs_.erase(clientID);
    catalogues_.erase(clientID);
}

CatalogueWriter& CatalogueHandler::catalogue(uint32_t id, const Key& dbKey) {
    std::lock_guard<std::mutex> lock(handlerMutex_);
    auto it = catalogues_.find(id);
    if (it == catalogues_.end()) {
        // n.b. building a catalogue writer creates the database: only once per client
        it = catalogues_.emplace(id, std::make_shared<CatalogueArchiver>(dbKey, innerConfig_)).first;
    }
    return *(it->second->catalogue);
}

std::shared_ptr<CatalogueArchiver> CatalogueHandler::catalogueArchiver(uint32_t clientID) {
    std::lock_guard<std::mutex> lock(handlerMutex_);
    auto it = catalogues_.find(clientID);
    return it == catalogues_.end() ? nullptr : it->second;
}

std::shared_ptr<FDB> CatalogueHandler::fdb(uint32_t clientID) {
    std::lock_guard<std::mutex> lock(handlerMutex_);
    auto it = fdbs_.find(clientID);
    if (it == fdbs_.end()) {
        it = fdbs_.emplace(clientID, std::make_shared<FDB>(innerConfig_)).first;
    }
    return it->second;
}

const CatalogueHandler::WipeInProgress& CatalogueHandler::cachedWipeState(Key dbKey) const {
    auto it = wipesInProgress_.find(dbKey);
    ASSERT(it != wipesInProgress_.end());
    return it->second;
}

void CatalogueHandler::doMaskIndexEntries(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {
    ASSERT(payload.size() > 0);
    MemoryStream s(payload);
    Key dbKey(s);
    std::lock_guard lock(wipesMutex_);
    const WipeInProgress& currentWipe = cachedWipeState(dbKey);
    currentWipe.catalogue->maskIndexEntries(currentWipe.state.indexesToMask());
}

void CatalogueHandler::doWipeURIs(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {
    ASSERT(payload.size() > 0);
    MemoryStream s(payload);
    Key dbKey(s);
    std::lock_guard lock(wipesMutex_);
    const WipeInProgress& currentWipe = cachedWipeState(dbKey);
    currentWipe.catalogue->doWipeURIs(currentWipe.state);
}

void CatalogueHandler::doWipeUnknowns(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) const {
    ASSERT(payload.size() > 0);
    MemoryStream s(payload);
    Key dbKey(s);
    std::lock_guard lock(wipesMutex_);
    const WipeInProgress& currentWipe = cachedWipeState(dbKey);

    std::set<eckit::URI> rec_unknownURIs;
    s >> rec_unknownURIs;

    // The client should not talk to us unless there are unknown URIs to wipe.
    ASSERT(!rec_unknownURIs.empty());

    // Unknown URIs are only wiped if unsafeWipeAll is set.
    ASSERT(currentWipe.unsafeWipeAll);

    std::set<eckit::URI> expected_unknownURIs = currentWipe.state.unrecognisedURIs();

    // Verify that the URIs we are being asked to delete is a subset of those previously identified as unknown.
    // Note: we are trusting the client to not be sending us anything later claimed by the stores.
    for (const auto& uri : rec_unknownURIs) {
        if (expected_unknownURIs.find(uri) == expected_unknownURIs.end()) {
            std::stringstream ss;
            ss << "CatalogueHandler::doWipeUnknowns: Received unknown URI " << uri
               << " which was not identified by the catalogue during wipe preparation";
            throw SeriousBug(ss.str(), Here());
        }
    }

    currentWipe.catalogue->doWipeUnknowns(rec_unknownURIs);
}

void CatalogueHandler::doWipeEmptyDatabase(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {
    ASSERT(payload.size() > 0);
    MemoryStream s(payload);
    Key dbKey(s);
    std::lock_guard lock(wipesMutex_);
    const WipeInProgress& currentWipe = cachedWipeState(dbKey);

    // Cleanup empty DBs and reset wipe state
    currentWipe.catalogue->doWipeEmptyDatabase();
    wipesInProgress_.erase(dbKey);
}

void CatalogueHandler::doUnsafeFullWipe(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {
    ASSERT(payload.size() > 0);
    MemoryStream s(payload);
    Key dbKey(s);
    std::lock_guard lock(wipesMutex_);
    const WipeInProgress& currentWipe = cachedWipeState(dbKey);

    bool fullWipeSupported = currentWipe.catalogue->doUnsafeFullWipe();

    eckit::Buffer boolBuf(5);
    eckit::MemoryStream stream(boolBuf);
    stream << fullWipeSupported;

    wipesInProgress_.erase(dbKey);

    write(Message::Received, true, clientID, requestID, boolBuf.data(), stream.position());
}

}  // namespace fdb5::remote
