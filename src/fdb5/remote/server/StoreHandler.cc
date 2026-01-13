/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/remote/server/StoreHandler.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/serialisation/ResizableMemoryStream.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/Store.h"
#include "fdb5/database/WipeState.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/remote/RemoteFieldLocation.h"
#include "fdb5/remote/server/ServerConnection.h"

#include "eckit/log/Log.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/types/Types.h"

#include "eckit/config/Resource.h"
#include "eckit/net/TCPSocket.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/types/Types.h"
#include "eckit/utils/Literals.h"

#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <utility>

using namespace eckit;
using namespace eckit::literals;

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

StoreHandler::StoreHandler(eckit::net::TCPSocket& socket, const Config& config) : ServerConnection(socket, config) {
    LibFdb5::instance().constructorCallback()(*this);
}

Handled StoreHandler::handleControl(Message message, uint32_t clientID, uint32_t requestID) {

    try {
        switch (message) {
            case Message::Store:  // notification that the client is starting to send data for archival
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
        error(e.what(), clientID, requestID);
    }
    catch (...) {
        error("Caught unexpected and unknown error", clientID, requestID);
    }
    return Handled::No;
}

Handled StoreHandler::handleControl(Message message, uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {

    try {
        switch (message) {

            case Message::Read:  // notification that the client is starting to send data location for read
                read(clientID, requestID, payload);
                return Handled::YesAddReadListener;

            case Message::Flush:  // flush store
                flush(clientID, requestID, payload);
                return Handled::Yes;

            case Message::Exists:  // given key (payload), check if store exists
                exists(clientID, requestID, payload);
                return Handled::Replied;

            case Message::Wipe:  // Initial wipe request
                prepareWipe(clientID, requestID, payload);
                return Handled::Replied;

            case Message::DoWipe:  // request to delete data marked for wipe
                doWipe(clientID, requestID, payload);
                return Handled::Yes;

            case Message::DoWipeFinish:  // request to delete empty databases and finish the wipe.
                doWipeFinish(clientID, requestID, payload);
                return Handled::Yes;

            case Message::DoWipeUnknowns:  // request to delete unknown URIs as part of a wipe
                doWipeUnknown(clientID, requestID, payload);
                return Handled::Yes;

            case Message::DoUnsafeFullWipe:  // request to delete full database and content
                doUnsafeFullWipe(clientID, requestID, payload);
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
        error(e.what(), clientID, requestID);
    }
    catch (...) {
        error("Caught unexpected and unknown error", clientID, requestID);
    }
    return Handled::No;
}

//----------------------------------------------------------------------------------------------------------------------

void StoreHandler::read(uint32_t clientID, uint32_t requestID, const eckit::Buffer& payload) {

    {
        std::lock_guard<std::mutex> lock(readLocationMutex_);
        if (!readLocationWorker_.joinable()) {
            readLocationWorker_ = std::thread([this] { readLocationThreadLoop(); });
        }
    }

    MemoryStream s(payload);

    std::unique_ptr<FieldLocation> location(eckit::Reanimator<FieldLocation>::reanimate(s));

    LOG_DEBUG_LIB(LibFdb5) << "Queuing for read: " << requestID << " " << *location << std::endl;

    std::unique_ptr<eckit::DataHandle> dh;
    dh.reset(location->dataHandle());

    readLocationQueue_.emplace(readLocationElem(clientID, requestID, std::move(dh)));
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

void StoreHandler::writeToParent(const uint32_t clientID, const uint32_t requestID,
                                 std::unique_ptr<eckit::DataHandle> dh) {
    try {
        Log::status() << "Reading: " << requestID << std::endl;
        // Write the data to the parent, in chunks if necessary.

        Buffer writeBuffer(4_MiB -
                           2_KiB);  // slightly smaller than 4MiB to nicely fit in a TCP window with scale factor 6
        long dataRead;

        dh->openForRead();
        LOG_DEBUG_LIB(LibFdb5) << "Reading: " << requestID << " dh size: " << dh->size() << std::endl;

        while ((dataRead = dh->read(writeBuffer, writeBuffer.size())) != 0) {
            write(Message::Blob, false, clientID, requestID, writeBuffer, dataRead);
        }

        // And when we are done, add a complete message.

        LOG_DEBUG_LIB(LibFdb5) << "Writing retrieve complete message: " << requestID << std::endl;

        write(Message::Complete, false, clientID, requestID);

        Log::status() << "Done retrieve: " << requestID << std::endl;
        LOG_DEBUG_LIB(LibFdb5) << "Done retrieve: " << requestID << std::endl;
    }
    catch (std::exception& e) {
        // n.b. more general than eckit::Exception
        error(e.what(), clientID, requestID);
    }
    catch (...) {
        // We really don't want to std::terminate the thread
        error("Caught unexpected , unknown exception in retrieve worker", clientID, requestID);
    }
}


void StoreHandler::archiveBlob(const uint32_t clientID, const uint32_t requestID, const void* data, size_t length) {

    MemoryStream s(data, length);

    fdb5::Key dbKey(s);
    fdb5::Key idxKey(s);

    std::ostringstream ss_key;
    ss_key << dbKey << idxKey;

    const char* charData = static_cast<const char*>(data);  // To allow pointer arithmetic
    Log::status() << "Archiving data: " << ss_key.str() << std::endl;

    Store& ss = store(clientID, dbKey);

    std::shared_ptr<const FieldLocation> location = ss.archive(idxKey, charData + s.position(), length - s.position());

    std::promise<std::shared_ptr<const FieldLocation>> promise;
    promise.set_value(location);

    eckit::StringDict dict = dbKey.keyDict();
    const auto& idxKeyDict = idxKey.keyDict();
    dict.insert(idxKeyDict.begin(), idxKeyDict.end());

    const Key fullkey(dict);  /// @note: we do not have the third level of the key.

    archiveCallback_(fullkey, charData + s.position(), length - s.position(), promise.get_future());

    Log::status() << "Archiving done: " << ss_key.str() << std::endl;

    eckit::Buffer buffer(16_KiB);
    MemoryStream stream(buffer);
    stream << (*location);
    Connection::write(Message::Store, true, clientID, requestID, buffer, stream.position());
}

void StoreHandler::flush(uint32_t clientID, uint32_t requestID, const eckit::Buffer& payload) {

    size_t numArchived = 0;


    if (requestID != 0) {
        ASSERT(payload.size() > 0);
        MemoryStream stream(payload);
        stream >> numArchived;
    }

    if (numArchived > 0) {
        ASSERT(archiveFuture_.valid());

        std::lock_guard<std::mutex> lock(handlerMutex_);
        auto it = stores_.find(clientID);
        ASSERT(it != stores_.end());
        it->second.store->flush();

        flushCallback_();
    }

    Log::info() << "Flush complete" << std::endl;
    Log::status() << "Flush complete" << std::endl;
}

bool StoreHandler::remove(bool control, uint32_t clientID) {

    std::lock_guard<std::mutex> lock(handlerMutex_);
    auto it = stores_.find(clientID);
    if (it != stores_.end()) {
        if (control) {
            it->second.controlConnection = false;
            numControlConnection_--;
        }
        else {
            it->second.dataConnection = false;
            numDataConnection_--;
        }
        if (!it->second.controlConnection && !it->second.dataConnection) {
            stores_.erase(it);
        }
    }
    return ((control ? numControlConnection_ : numDataConnection_) == 0);
}

Store& StoreHandler::getStore(uint32_t clientID) {

    std::lock_guard<std::mutex> lock(handlerMutex_);
    auto it = stores_.find(clientID);
    if (it == stores_.end()) {
        std::string what("Requested Store has not been loaded id: " + std::to_string(clientID));
        Log::error() << what << std::endl;
        Log::error() << stores_.size() << " existing stores:" << std::endl;
        for (const auto& kv : stores_) {
            Log::error() << "  clientID: " << kv.first << ", store: " << *(kv.second.store) << std::endl;
        }
        write(Message::Error, true, 0, 0, what.c_str(), what.length());
        throw;
    }

    return *(it->second.store);
}

Store& StoreHandler::store(uint32_t clientID, const Key& dbKey) {

    std::lock_guard<std::mutex> lock(handlerMutex_);
    auto it = stores_.find(clientID);
    if (it != stores_.end()) {
        return *(it->second.store);
    }

    numControlConnection_++;
    if (!single_) {
        numDataConnection_++;
    }
    return *((stores_.emplace(clientID, StoreHelper(!single_, dbKey, config_)).first)->second.store);
}

Store& StoreHandler::getStore(uint32_t clientID, const eckit::URI& uri) {

    std::lock_guard<std::mutex> lock(handlerMutex_);
    auto it = stores_.find(clientID);
    if (it != stores_.end()) {
        return *(it->second.store);
    }

    numControlConnection_++;
    if (!single_) {
        numDataConnection_++;
    }
    return *((stores_.emplace(clientID, StoreHelper(!single_, uri, config_)).first)->second.store);
}

void StoreHandler::exists(const uint32_t clientID, const uint32_t requestID, const eckit::Buffer& payload) const {

    ASSERT(payload.size() > 0);

    bool exists = false;

    {
        eckit::MemoryStream stream(payload);
        const Key dbKey(stream);
        exists = StoreFactory::instance().build(dbKey, config_)->exists();
    }

    eckit::Buffer existBuf(5);
    eckit::MemoryStream stream(existBuf);
    stream << exists;

    write(Message::Received, true, clientID, requestID, existBuf.data(), stream.position());
}

void StoreHandler::doWipeUnknown(const uint32_t clientID, const uint32_t requestID, const eckit::Buffer& payload) {
    eckit::MemoryStream s(payload);
    Key key(s);
    const WipeInProgress& currentWipe = cachedWipeState(key);

    std::set<eckit::URI> uris{s};

    // Only proceed if unsafeWipeAll is set.
    ASSERT(currentWipe.unsafeWipeAll);

    auto& ss = getStore(clientID);

    // check received URIs are at least a subset of expected unknowns.
    const auto& expectedUnknowns = currentWipe.state.unrecognisedURIs();
    for (const auto& uri : uris) {
        if (expectedUnknowns.find(uri) == expectedUnknowns.end()) {
            std::stringstream ss;
            ss << "StoreHandler::doWipeUnknown: Received unknown URI " << uri << " which was not expected to be wiped.";
            Log::status() << ss.str() << std::endl;
            Log::error() << ss.str() << std::endl;
            throw SeriousBug(ss.str(), Here());
        }
    }

    ss.doWipeUnknownContents(uris);
}

void StoreHandler::doWipe(const uint32_t clientID, const uint32_t requestID, const eckit::Buffer& payload) {
    eckit::MemoryStream s(payload);
    Key key(s);
    const WipeInProgress& currentWipe = cachedWipeState(key);

    auto& store = getStore(clientID);
    store.doWipe(currentWipe.state);
}

void StoreHandler::doWipeFinish(const uint32_t clientID, const uint32_t requestID, const eckit::Buffer& payload) {
    eckit::MemoryStream s(payload);
    Key key(s);
    const WipeInProgress& currentWipe = cachedWipeState(key);

    // Delete empty DBs and finish the wipe.

    auto& store = getStore(clientID);
    store.doWipeEmptyDatabases();

    wipesInProgress_.erase(key);
}

void StoreHandler::doUnsafeFullWipe(const uint32_t clientID, const uint32_t requestID, const eckit::Buffer& payload) {
    ASSERT(payload.size() > 0);
    MemoryStream s(payload);
    Key key(s);
    const WipeInProgress& currentWipe = cachedWipeState(key);

    auto& store            = getStore(clientID);
    bool fullWipeSupported = store.doUnsafeFullWipe();

    eckit::Buffer boolBuf(5);
    eckit::MemoryStream stream(boolBuf);
    stream << fullWipeSupported;

    wipesInProgress_.erase(key);

    write(Message::Received, true, clientID, requestID, boolBuf.data(), stream.position());
}

const StoreHandler::WipeInProgress& StoreHandler::cachedWipeState(const Key& uri) const {
    auto it = wipesInProgress_.find(uri);
    ASSERT(it != wipesInProgress_.end());
    return it->second;
}

void StoreHandler::prepareWipe(const uint32_t clientID, const uint32_t requestID, const eckit::Buffer& payload) {

    bool unsafeAll = false;
    bool doit      = false;
    eckit::MemoryStream inStream(payload);

    Key dbkey(inStream);
    StoreWipeState inState(inStream);
    inStream >> unsafeAll;
    inStream >> doit;

    // XXX Validate signature.
    const std::string dummy_secret = eckit::Resource<std::string>("$FDB_WIPE_SECRET;fdbWipeSecret", "");
    ASSERT(!dummy_secret.empty());

    uint64_t expected_hash = inState.hash(dummy_secret);
    ASSERT(inState.signature().validSignature(expected_hash));

    // -- From here on, we can trust the state came from the catalogue. --

    // The URIs need to be converted to internal URIs for this store.
    StoreWipeState storeState(RemoteFieldLocation::internalURI(inState.storeURI()));

    for (const auto& uri : inState.includedDataURIs()) {
        storeState.includeData(RemoteFieldLocation::internalURI(uri));
    }
    for (const auto& uri : inState.safeURIs()) {
        storeState.excludeData(RemoteFieldLocation::internalURI(uri));
    }

    if (storeState.includedDataURIs().empty()) {
        // Client should not communicate with the store if there are no data URIs to wipe.
        error("Wipe request has no data URIs", clientID, requestID);
        return;
    }

    auto& store = getStore(clientID, *(storeState.includedDataURIs().begin()));
    store.prepareWipe(storeState, doit, unsafeAll);

    // Write back with the additional URIs to be wiped.
    eckit::Buffer outBuffer(1_MiB);
    eckit::ResizableMemoryStream outStream(outBuffer);
    outStream << storeState.dataAuxiliaryURIs();
    outStream << storeState.unrecognisedURIs();

    write(Message::Wipe, true, clientID, requestID, outBuffer.data(), outStream.position());

    // keep state for doWipe
    if (doit) {
        ASSERT(!unsafeAll);  // Until Im explicitly told otherwise, we dont support unsafeAll on remote fdb.
        wipesInProgress_[dbkey] = {
            unsafeAll,
            std::move(storeState),
        };
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
