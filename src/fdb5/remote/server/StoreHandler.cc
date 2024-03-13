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

namespace fdb5::remote {

StoreHandler::StoreHandler(eckit::net::TCPSocket& socket, const Config& config):
    ServerConnection(socket, config) {}

StoreHandler::~StoreHandler() {}

Handled StoreHandler::handleControl(Message message, uint32_t clientID, uint32_t requestID) {

    try {
        switch (message) {
            case Message::Store: // notification that the client is starting to send data for archival
                archiver();
                return Handled::YesAddArchiveListener;
            
            // case Message::Flush: // notification that the client has sent all data for archival
            //     flush(clientID, requestID, eckit::Buffer{0});
            //     return Handled::YesRemoveArchiveListener; // ????

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

Handled StoreHandler::handleControl(Message message, uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {

    try {
        switch (message) {

            case Message::Read: // notification that the client is starting to send data location for read
                read(clientID, requestID, payload);
                return Handled::YesAddReadListener;

            case Message::Flush: // flush store
                flush(clientID, requestID, payload);
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

void StoreHandler::read(uint32_t clientID, uint32_t requestID, const eckit::Buffer& payload) {

    if (!readLocationWorker_.joinable()) {
        readLocationWorker_ = std::thread([this] { readLocationThreadLoop(); });
    }

    MemoryStream s(payload);

    std::unique_ptr<FieldLocation> location(eckit::Reanimator<FieldLocation>::reanimate(s));

    Log::debug<LibFdb5>() << "Queuing for read: " << requestID << " " << *location << std::endl;

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
            write(Message::Blob, false, clientID, requestID, std::vector<std::pair<const void*, uint32_t>>{{writeBuffer, dataRead}});
        }

        // And when we are done, add a complete message.

        Log::debug<LibFdb5>() << "Writing retrieve complete message: " << requestID
                              << std::endl;

        write(Message::Complete, false, clientID, requestID);

        Log::status() << "Done retrieve: " << requestID << std::endl;
        Log::debug<LibFdb5>() << "Done retrieve: " << requestID << std::endl;
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

    std::stringstream ss_key;
    ss_key << dbKey << idxKey;

    const char* charData = static_cast<const char*>(data);  // To allow pointer arithmetic
    Log::status() << "Archiving data: " << ss_key.str() << std::endl;
    
    Store& ss = store(clientID, dbKey);

    std::unique_ptr<FieldLocation> location = ss.archive(idxKey, charData + s.position(), length - s.position());
    Log::status() << "Archiving done: " << ss_key.str() << std::endl;
    
    eckit::Buffer buffer(16 * 1024);
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

    ASSERT(numArchived == 0 || archiveFuture_.valid());

    auto it = stores_.find(clientID);
    ASSERT(it != stores_.end());
    it->second.store->flush();

    // if (archiveFuture_.valid()) {
    //     // Ensure that the expected number of fields have been written, and that the
    //     // archive worker processes have been cleanly wound up.
    //     size_t n = archiveFuture_.get();
    //     ASSERT(numArchived == n);

    //     // Do the actual flush!
    //     Log::info() << "Flushing" << std::endl;
    //     Log::status() << "Flushing" << std::endl;
    //     // for (auto it = stores_.begin(); it != stores_.end(); it++) {
    //     //     it->second->flush();
    //     // }
    // }

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
        } else {
            it->second.dataConnection = false;
            numDataConnection_--;
        }
        if (!it->second.controlConnection && !it->second.dataConnection) {
            stores_.erase(it);
        }
    }
    return ((control ? numControlConnection_ : numDataConnection_) == 0);
}

// bool StoreHandler::handlers() {
//     std::lock_guard<std::mutex> lock(handlerMutex_);
//     return stores_.empty();
// }

Store& StoreHandler::store(uint32_t clientID) {

    std::lock_guard<std::mutex> lock(handlerMutex_);
    auto it = stores_.find(clientID);
    if (it == stores_.end()) {
        std::string what("Requested Store has not been loaded id: " + std::to_string(clientID));
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

}  // namespace fdb5::remote
