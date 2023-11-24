/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <dirent.h>
#include <fcntl.h>

#include "eckit/log/Timer.h"

#include "eckit/config/Resource.h"
#include "eckit/io/AIOHandle.h"
#include "eckit/io/EmptyHandle.h"
#include "eckit/runtime/Main.h"
#include "eckit/serialisation/MemoryStream.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/remote/RemoteStore.h"
#include "fdb5/remote/RemoteFieldLocation.h"
#include "fdb5/io/FDBFileHandle.h"

using namespace eckit;

//----------------------------------------------------------------------------------------------------------------------

namespace fdb5::remote {

class StoreArchivalRequest {

public:

    StoreArchivalRequest() :
        id_(0), store_(nullptr), key_(Key()), buffer_(Buffer()) {}

    StoreArchivalRequest(uint32_t id, RemoteStore* store, const fdb5::Key& key, const void *data, eckit::Length length) :
        id_(id), store_(store), key_(key), buffer_(Buffer(reinterpret_cast<const char*>(data), length)) {}

    uint32_t id_;
    RemoteStore* store_;
    fdb5::Key key_;
    eckit::Buffer buffer_;
};

//----------------------------------------------------------------------------------------------------------------------

class RemoteStoreArchiver {

public: // types

    using StoreArchiveQueue = eckit::Queue<StoreArchivalRequest>;

public: // methods

    static RemoteStoreArchiver* get(const eckit::net::Endpoint& endpoint);

    bool valid() { return archiveFuture_.valid(); }
    bool dirty() { return dirty_; }

    void start();
    void error(eckit::Buffer&& payload, const eckit::net::Endpoint& endpoint);
    
    void emplace(uint32_t id, RemoteStore* store, const Key& key, const void *data, eckit::Length length);
    FDBStats flush(RemoteStore* store);

private: // methods

    RemoteStoreArchiver() : dirty_(false), maxArchiveQueueLength_(eckit::Resource<size_t>("fdbRemoteArchiveQueueLength;$FDB_REMOTE_ARCHIVE_QUEUE_LENGTH", 200)),
    archiveQueue_(nullptr) {}

    FDBStats archiveThreadLoop();

private: // members

    bool dirty_;

    std::mutex archiveQueuePtrMutex_;
    size_t maxArchiveQueueLength_;
    std::unique_ptr<StoreArchiveQueue> archiveQueue_;
    std::future<FDBStats> archiveFuture_;

};

RemoteStoreArchiver* RemoteStoreArchiver::get(const eckit::net::Endpoint& endpoint) {

    static std::map<const std::string, std::unique_ptr<RemoteStoreArchiver>> archivers_;

    auto it = archivers_.find(endpoint.hostport());
    if (it == archivers_.end()) {
        // auto arch = (archivers_[endpoint.hostport()] = RemoteStoreArchiver());
        it = archivers_.emplace(endpoint.hostport(), new RemoteStoreArchiver()).first;
    }
    ASSERT(it != archivers_.end());
    return it->second.get();
}

void RemoteStoreArchiver::start() {
    // if there is no archiving thread active, then start one.
    // n.b. reset the archiveQueue_ after a potential flush() cycle.
    if (!archiveFuture_.valid()) {

        {
            // Reset the queue after previous done/errors
            std::lock_guard<std::mutex> lock(archiveQueuePtrMutex_);
            ASSERT(!archiveQueue_);
            archiveQueue_.reset(new StoreArchiveQueue(maxArchiveQueueLength_));
        }

        archiveFuture_ = std::async(std::launch::async, [this] { return archiveThreadLoop(); });
    }
}

void RemoteStoreArchiver::error(eckit::Buffer&& payload, const eckit::net::Endpoint& endpoint) {

    std::lock_guard<std::mutex> lock(archiveQueuePtrMutex_);

    if (archiveQueue_) {
        std::string msg;
        if (payload.size() > 0) {
            msg.resize(payload.size(), ' ');
            payload.copy(&msg[0], payload.size());
        }

        archiveQueue_->interrupt(std::make_exception_ptr(RemoteFDBException(msg, endpoint)));
    }
}

void RemoteStoreArchiver::emplace(uint32_t id, RemoteStore* store, const Key& key, const void *data, eckit::Length length) {

    dirty_ = true;

    std::lock_guard<std::mutex> lock(archiveQueuePtrMutex_);
    ASSERT(archiveQueue_);
    archiveQueue_->emplace(id, store, key, data, length);
}

FDBStats RemoteStoreArchiver::flush(RemoteStore* store) {

    std::lock_guard<std::mutex> lock(archiveQueuePtrMutex_);
    ASSERT(archiveQueue_);
    archiveQueue_->close();

    FDBStats stats = archiveFuture_.get();
    ASSERT(!archiveQueue_);

    ASSERT(stats.numFlush() == 0);
    size_t numArchive = stats.numArchive();

    Buffer sendBuf(1024);
    MemoryStream s(sendBuf);
    s << numArchive;

    // The flush call is blocking
    store->controlWriteCheckResponse(Message::Flush, sendBuf, s.position());

    dirty_ = false;

    return stats;
}

FDBStats RemoteStoreArchiver::archiveThreadLoop() {
    FDBStats localStats;
    eckit::Timer timer;

    StoreArchivalRequest element;

    try {

        ASSERT(archiveQueue_);
        long popped;
        while ((popped = archiveQueue_->pop(element)) != -1) {

            eckit::Log::debug<LibFdb5>() << "RemoteStoreArchiver archiving [id="<<element.id_<<",key="<<element.key_<<",payloadSize="<<element.buffer_.size()<<"]" << std::endl;

            timer.start();
            element.store_->sendArchiveData(element.id_, element.key_, element.buffer_.data(), element.buffer_.size());
            timer.stop();

            localStats.addArchive(element.buffer_.size(), timer, 1);
        }

        // And note that we are done. (don't time this, as already being blocked
        // on by the ::flush() routine)
        element.store_->dataWrite(Message::Flush, 0);

        archiveQueue_.reset();

    } catch (...) {
        archiveQueue_->interrupt(std::current_exception());
        throw;
    }

    return localStats;

    // We are inside an async, so don't need to worry about exceptions escaping.
    // They will be released when flush() is called.
}


// -----------------------------------------------------------------------------------------------------

//
/// @note The DataHandles returned by retrieve() MUST STRICTLY be read in order.
///       We do not create multiple message queues, one for each requestID, even
///       though that would be nice. This is because a commonly used retrieve
///       pattern uses many retrieve() calls aggregated into a MultiHandle, and
///       if we created lots of queues we would just run out of memory receiving
///       from the stream. Further, if we curcumvented this by blocking, then we
///       could get deadlocked if we try and read a message that is further back
///       in the stream
///
/// --> Retrieve is a _streaming_ service.
    

class FDBRemoteDataHandle : public DataHandle {

public: // methods

    FDBRemoteDataHandle(uint32_t requestID,
                        RemoteStore::MessageQueue& queue,
                        const net::Endpoint& remoteEndpoint) :
        requestID_(requestID),
        queue_(queue),
        remoteEndpoint_(remoteEndpoint),
        pos_(0),
        overallPosition_(0),
        currentBuffer_(0),
        complete_(false) {}
    virtual bool canSeek() const override { return false; }

private: // methods

    void print(std::ostream& s) const override {
        s << "FDBRemoteDataHandle(id=" << requestID_ << ")";
    }

    Length openForRead() override {
        return estimate();
    }
    void openForWrite(const Length&) override { NOTIMP; }
    void openForAppend(const Length&) override { NOTIMP; }
    long write(const void*, long) override { NOTIMP; }
    void close() override {}

    long read(void* pos, long sz) override {

        if (complete_) return 0;

        if (currentBuffer_.size() != 0) return bufferRead(pos, sz);

        // If we are in the DataHandle, then there MUST be data to read

        RemoteStore::StoredMessage msg = std::make_pair(remote::Message{}, eckit::Buffer{0});
        ASSERT(queue_.pop(msg) != -1);

        // Handle any remote errors communicated from the server
        if (msg.first == Message::Error) {
            std::string errmsg(static_cast<const char*>(msg.second.data()), msg.second.size());
            throw RemoteFDBException(errmsg, remoteEndpoint_);
        }

        // Are we now complete?
        if (msg.first == Message::Complete) {
            complete_ = 0;
            return 0;
        }

        ASSERT(msg.first == Message::Blob);

        // Otherwise return the data!
        std::swap(currentBuffer_, msg.second);

        return bufferRead(pos, sz);
    }

    // A helper function that returns some, or all, of a buffer that has
    // already been retrieved.

    long bufferRead(void* pos, long sz) {

        ASSERT(currentBuffer_.size() != 0);
        ASSERT(pos_ < currentBuffer_.size());

        long read = std::min(sz, long(currentBuffer_.size() - pos_));

        ::memcpy(pos, &currentBuffer_[pos_], read);
        pos_ += read;
        overallPosition_ += read;

        // If we have exhausted this buffer, free it up.

        if (pos_ >= currentBuffer_.size()) {
            Buffer nullBuffer(0);
            std::swap(currentBuffer_, nullBuffer);
            pos_ = 0;
            ASSERT(currentBuffer_.size() == 0);
        }

        return read;
    }

    Length estimate() override {
        return 0;
    }

    Offset position() override {
        return overallPosition_;
    }

private: // members

    uint32_t requestID_;
    RemoteStore::MessageQueue& queue_;
    net::Endpoint remoteEndpoint_;
    size_t pos_;
    Offset overallPosition_;
    Buffer currentBuffer_;
    bool complete_;
};

// std::vector<eckit::net::Endpoint> writeEndpoints(const Config& config) {
//     if (config.has("storeHost") && config.has("storePort")) {
//         return std::vector{eckit::net::Endpoint(config.getString("storeHost"), config.getInt("storePort"))};
//     }
//     ASSERT(config.has("stores"));
//     std::vector<eckit::LocalConfiguration> stores = config.getSubConfigurations("stores");
//     for (const auto& root : stores) {
//         spaceRoots.emplace_back(
//             Root(
//                 root.getString("path"),
//                 root.getString("name", ""),
//                 root.getBool("list", visit),
//                 root.getBool("retrieve", visit),
//                 root.getBool("archive", writable),
//                 root.getBool("wipe", writable)
//             )
//         );
//     }

//     .getStringVector("stores");

//     std::srand(std::time(nullptr));
//     return eckit::net::Endpoint(stores.at(std::rand() % stores.size()));
// }

eckit::net::Endpoint storeEndpoint(const Config& config) {
    if (config.has("storeHost") && config.has("storePort")) {
        return eckit::net::Endpoint(config.getString("storeHost"), config.getInt("storePort"));
    }
    ASSERT(config.has("stores"));
    std::vector<std::string> stores = config.getStringVector("stores");
    std::srand(std::time(nullptr));
    return eckit::net::Endpoint(stores.at(std::rand() % stores.size()));
}

//----------------------------------------------------------------------------------------------------------------------

RemoteStore::RemoteStore(const Key& dbKey, const Config& config) :
    Client(storeEndpoint(config)),
    dbKey_(dbKey), config_(config),
    retrieveMessageQueue_(eckit::Resource<size_t>("fdbRemoteRetrieveQueueLength;$FDB_REMOTE_RETRIEVE_QUEUE_LENGTH", 200)) {

    if (config.has("localStores")) {
        for (const std::string& localStore : config.getStringVector("localStores")) {
            if (localStore == endpoint_.hostport()) {
                local_ = true;
                break;
            }
        }
    }
}

RemoteStore::RemoteStore(const eckit::URI& uri, const Config& config) :
    Client(eckit::net::Endpoint(uri.hostport())),
    dbKey_(Key()), config_(config),
    retrieveMessageQueue_(eckit::Resource<size_t>("fdbRemoteRetrieveQueueLength;$FDB_REMOTE_RETRIEVE_QUEUE_LENGTH", 200)) {

    // no need to set the local_ flag on the read path

    // if (config.has("localStores")) {
    //     for (const std::string& localStore : config.getStringVector("localStores")) {
    //         if (localStore == endpoint_.hostport()) {
    //             local_ = true;
    //             break;
    //         }
    //     }
    // }

    ASSERT(uri.scheme() == "fdb");
}

RemoteStore::~RemoteStore() {
    // If we have launched a thread with an async and we manage to get here, this is
    // an error. n.b. if we don't do something, we will block in the destructor
    // of std::future.
    if (archiver_ && archiver_->valid()) {
        Log::error() << "Attempting to destruct DecoupledFDB with active archive thread" << std::endl;
        eckit::Main::instance().terminate();
    }

    ASSERT(!archiver_ || !archiver_->dirty());
//    disconnect();
}

eckit::URI RemoteStore::uri() const {
    return URI("fdb", "");
}

bool RemoteStore::exists() const {
    return true; // directory_.exists();
}

eckit::DataHandle* RemoteStore::retrieve(Field& field) const {
    return field.dataHandle();
}

void RemoteStore::archive(const Key& key, const void *data, eckit::Length length, std::function<void(const std::unique_ptr<FieldLocation> fieldLocation)> catalogue_archive) {

    // if there is no archiving thread active, then start one.
    // n.b. reset the archiveQueue_ after a potential flush() cycle.
    if (!archiver_) {
        archiver_ = RemoteStoreArchiver::get(controlEndpoint());
    }
    ASSERT(archiver_);
    archiver_->start();
    ASSERT(archiver_->valid());

    uint32_t id = controlWriteCheckResponse(Message::Archive, nullptr, 0);
    locations_[id] = catalogue_archive;
    archiver_->emplace(id, this, key, data, length);
}

bool RemoteStore::open() {
    return true;
}

void RemoteStore::flush() {

    Timer timer;

    timer.start();

    // Flush only does anything if there is an ongoing archive();
    if (archiver_->valid()) {
        archiver_->flush(this);
//        internalStats_ += stats;
    }

    timer.stop();
//    internalStats_.addFlush(timer);
}

void RemoteStore::close() {
//    disconnect();
}

void RemoteStore::remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const {
    NOTIMP;    
}

void RemoteStore::remove(const Key& key) const {
    NOTIMP;
}

void RemoteStore::print(std::ostream &out) const {
    out << "RemoteStore(host=" << /*controlEndpoint() << ", data=" << dataEndpoint() << */ ")";
}

bool RemoteStore::handle(Message message, uint32_t requestID) {

    switch (message) {  
        case Message::Complete: {
            auto it = messageQueues_.find(requestID);
            if (it != messageQueues_.end()) {
                it->second->close();

                // Remove entry (shared_ptr --> message queue will be destroyed when it
                // goes out of scope in the worker thread).
                messageQueues_.erase(it);

            } else {
                retrieveMessageQueue_.emplace(std::make_pair(message, Buffer(0)));
            }
            return true;
        }
        case Message::Error: {

            auto it = messageQueues_.find(requestID);
            if (it != messageQueues_.end()) {
                it->second->interrupt(std::make_exception_ptr(RemoteFDBException("", endpoint_)));

                // Remove entry (shared_ptr --> message queue will be destroyed when it
                // goes out of scope in the worker thread).
                messageQueues_.erase(it);
                return true;

            }
            return false;
        }
        default:
            return false;
    }
}
bool RemoteStore::handle(Message message, uint32_t requestID, eckit::Buffer&& payload) {

    switch (message) {
    
        case Message::Archive: {
            auto it = locations_.find(requestID);
            if (it != locations_.end()) {
                MemoryStream s(payload);
                std::unique_ptr<FieldLocation> location(eckit::Reanimator<FieldLocation>::reanimate(s));
                if (local_) {
                    it->second(std::move(location));
                } else {
                    // std::cout <<  "RemoteStore::handle - " << location->uri().asRawString() << " " << location->length() << std::endl;
                    std::unique_ptr<RemoteFieldLocation> remoteLocation = std::unique_ptr<RemoteFieldLocation>(new RemoteFieldLocation(endpoint_, *location));
                    it->second(std::move(remoteLocation));
                }
            }
            return true;
        }
        case Message::Blob: {
            auto it = messageQueues_.find(requestID);
            if (it != messageQueues_.end()) {
                it->second->emplace(message, std::move(payload));
            } else {
                retrieveMessageQueue_.emplace(message, std::move(payload));
            }
            return true;
        }
        case Message::Error: {

            auto it = messageQueues_.find(requestID);
            if (it != messageQueues_.end()) {
                std::string msg;
                msg.resize(payload.size(), ' ');
                payload.copy(&msg[0], payload.size());
                it->second->interrupt(std::make_exception_ptr(RemoteFDBException(msg, endpoint_)));

                // Remove entry (shared_ptr --> message queue will be destroyed when it
                // goes out of scope in the worker thread).
                messageQueues_.erase(it);

            } else {
                auto it = locations_.find(requestID);
                if (it != locations_.end()) {
                    archiver_->error(std::move(payload), endpoint_);
                } else {
                    retrieveMessageQueue_.emplace(message, std::move(payload));
                }
            }
            return true;
        }
        default:
            return false;
    }
}
void RemoteStore::handleException(std::exception_ptr e) {
    Log::error() << "RemoteStore::handleException " << std::endl;
}

void RemoteStore::flush(FDBStats& internalStats) {
    // Flush only does anything if there is an ongoing archive();
    if (! archiver_->valid()) return;

    internalStats += archiver_->flush(this);
}

void RemoteStore::sendArchiveData(uint32_t id, const Key& key, const void* data, size_t length) {
    
    ASSERT(!dbKey_.empty());
    ASSERT(!key.empty());

    ASSERT(data);
    ASSERT(length != 0);

    Buffer keyBuffer(4096);
    MemoryStream keyStream(keyBuffer);
    keyStream << dbKey_;
    keyStream << key;

    std::vector<std::pair<const void*, uint32_t>> payloads;
    payloads.push_back(std::pair<const void*, uint32_t>{keyBuffer, keyStream.position()});
    payloads.push_back(std::pair<const void*, uint32_t>{data, length});

    dataWrite(Message::Blob, id, payloads);
}

eckit::DataHandle* RemoteStore::dataHandle(const FieldLocation& fieldLocation) {
    return dataHandle(fieldLocation, Key());
}

eckit::DataHandle* RemoteStore::dataHandle(const FieldLocation& fieldLocation, const Key& remapKey) {

    Buffer encodeBuffer(4096);
    MemoryStream s(encodeBuffer);
    s << fieldLocation;
    s << remapKey;

    uint32_t id = controlWriteCheckResponse(fdb5::remote::Message::Read, encodeBuffer, s.position());

    return new FDBRemoteDataHandle(id, retrieveMessageQueue_, endpoint_);
}

RemoteStore& RemoteStore::get(const eckit::URI& uri) {
    // we memoise one read store for each endpoint. Do not need to have one for each key
    static std::map<std::string, std::unique_ptr<RemoteStore>> readStores_;

    const std::string& endpoint = uri.hostport();
    auto it = readStores_.find(endpoint);
    if (it != readStores_.end()) {
        return *(it->second);
    }

    return *(readStores_[endpoint] = std::unique_ptr<RemoteStore>(new RemoteStore(uri, Config())));
}

static StoreBuilder<RemoteStore> builder("remote");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5::remote
