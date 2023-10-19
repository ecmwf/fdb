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
#include "fdb5/remote/client/ClientConnectionRouter.h"
#include "fdb5/io/FDBFileHandle.h"

using namespace eckit;

namespace {
    // // n.b. if we get integer overflow, we reuse the IDs. This is not a
// //      big deal. The idea that we could be on the 2.1 billionth (successful)
// //      request, and still have an ongoing request 0 is ... laughable.
static uint32_t generateRequestID() {

    static std::mutex m;
    static uint32_t id = 0;

    std::lock_guard<std::mutex> lock(m);
    return ++id;
}

}
namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

RemoteStore::RemoteStore(const Key& dbKey, const Config& config) :
    Client(std::vector<eckit::net::Endpoint>{eckit::net::Endpoint("localhost", 7000)}),
//    Client(std::vector<Connection>{ClientConnectionRouter::instance().connectStore(dbKey_, std::vector<eckit::net::Endpoint>{eckit::net::Endpoint("localhost", 7000)})}),
//    ClientConnection(eckit::net::Endpoint(config.getString("storeHost"), config.getInt("storePort")), config),
    dbKey_(dbKey), config_(config), dirty_(false), //archiveID_(0),
    maxArchiveQueueLength_(eckit::Resource<size_t>("fdbRemoteArchiveQueueLength;$FDB_REMOTE_ARCHIVE_QUEUE_LENGTH", 200)),
    retrieveMessageQueue_(eckit::Resource<size_t>("fdbRemoteRetrieveQueueLength;$FDB_REMOTE_RETRIEVE_QUEUE_LENGTH", 200)),
    maxArchiveBatchSize_(config.getInt("maxBatchSize", 1)) {
}

RemoteStore::RemoteStore(const eckit::URI& uri, const Config& config) :
    Client(std::vector<eckit::net::Endpoint>{eckit::net::Endpoint("localhost", 7000)}),
//    Client(std::vector<Connection>{ClientConnectionRouter::instance().connectStore(Key(), std::vector<eckit::net::Endpoint>{eckit::net::Endpoint("localhost", 7000)})}),
    dbKey_(Key()), config_(config), dirty_(false), //archiveID_(0),
    maxArchiveQueueLength_(eckit::Resource<size_t>("fdbRemoteArchiveQueueLength;$FDB_REMOTE_ARCHIVE_QUEUE_LENGTH", 200)),
    retrieveMessageQueue_(eckit::Resource<size_t>("fdbRemoteRetrieveQueueLength;$FDB_REMOTE_RETRIEVE_QUEUE_LENGTH", 200)),
    maxArchiveBatchSize_(config.getInt("maxBatchSize", 1)) {

    std::cout << "RemoteStore ctor " << uri.asRawString() << std::endl;

    ASSERT(uri.scheme() == "fdbremote");
    // std::vector<eckit::net::Endpoint> endpoints{eckit::net::Endpoint(uri.hostport())};
    // connection_ = ClientConnectionRouter::instance().connectStore(dbKey_, endpoints);
}

RemoteStore::~RemoteStore() {
    // If we have launched a thread with an async and we manage to get here, this is
    // an error. n.b. if we don't do something, we will block in the destructor
    // of std::future.
    if (archiveFuture_.valid()) {
        Log::error() << "Attempting to destruct DecoupledFDB with active archive thread" << std::endl;
        eckit::Main::instance().terminate();
    }

    ASSERT(!dirty_);
//    disconnect();
}

eckit::URI RemoteStore::uri() const {
    return URI("fdbremote", "");
}

bool RemoteStore::exists() const {
    return true; // directory_.exists();
}

eckit::DataHandle* RemoteStore::retrieve(Field& field) const {
    //return field.dataHandle();
    return nullptr;
}

std::future<std::unique_ptr<FieldLocation> > RemoteStore::archive(const Key& key, const void *data, eckit::Length length) {
    // dirty_ = true;
    // uint32_t id = generateRequestID();

//    connect();

    // if there is no archiving thread active, then start one.
    // n.b. reset the archiveQueue_ after a potential flush() cycle.
    if (!archiveFuture_.valid()) {

        // Start the archival request on the remote side

        // Reset the queue after previous done/errors
        {
            std::lock_guard<std::mutex> lock(archiveQueuePtrMutex_);
            ASSERT(!archiveQueue_);
            archiveQueue_.reset(new ArchiveQueue(maxArchiveQueueLength_));
        }

        archiveFuture_ = std::async(std::launch::async, [this] { return archiveThreadLoop(); });
    }

    ASSERT(archiveFuture_.valid());

    uint32_t id = controlWriteCheckResponse(Message::Archive);
    std::lock_guard<std::mutex> lock(archiveQueuePtrMutex_);
    ASSERT(archiveQueue_);
    // std::vector<Key> combinedKey = {dbKey_, key};
    // std::cout << "archiveID_: " << archiveID_ << "  Archiving " << combinedKey << std::endl;
    archiveQueue_->emplace(std::make_pair(id, std::make_pair(key, Buffer(reinterpret_cast<const char*>(data), length))));

    std::promise<std::unique_ptr<FieldLocation> > loc;
    auto futureLocation = loc.get_future();
    locations_[id] = std::move(loc);
    return futureLocation;
}

bool RemoteStore::open() {
//    connect();
    return true;
}


void RemoteStore::flush() {

    Timer timer;

    timer.start();

    // Flush only does anything if there is an ongoing archive();
    if (archiveFuture_.valid()) {

        ASSERT(archiveQueue_);
        std::lock_guard<std::mutex> lock(archiveQueuePtrMutex_);
        archiveQueue_->close();

        FDBStats stats = archiveFuture_.get();
        ASSERT(!archiveQueue_);

        ASSERT(stats.numFlush() == 0);
        size_t numArchive = stats.numArchive();

        Buffer sendBuf(4096);
        MemoryStream s(sendBuf);
        s << numArchive;

        // The flush call is blocking
        controlWriteCheckResponse(fdb5::remote::Message::Flush, sendBuf, s.position());

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




//     eckit::PathName src_db = directory_ / key.valuesToString();
        
//     DIR* dirp = ::opendir(src_db.asString().c_str());
//     struct dirent* dp;
//     while ((dp = ::readdir(dirp)) != NULL) {
//         if (strstr( dp->d_name, ".data")) {
//             eckit::PathName dataFile = src_db / dp->d_name;
//             eckit::Log::debug<LibFdb5>() << "Removing " << dataFile << std::endl;
//             dataFile.unlink(false);
//         }
//     }
//     closedir(dirp);
// }

// void RemoteStore::connect() {
//     ASSERT(remote_);
//     remote_->connect();
// }

// void RemoteStore::disconnect() {
//     if (remote_) {
//         remote_->disconnect();
//     }
// }

void RemoteStore::print(std::ostream &out) const {
    out << "RemoteStore(host=" << /*controlEndpoint() << ", data=" << dataEndpoint() << */ ")";
}





// void RemoteStore::flush() {

//     Timer timer;

//     timer.start();

//     // Flush only does anything if there is an ongoing archive();
//     if (archiveFuture_.valid()) {

//         ASSERT(archiveID_ != 0);
//         {
//             ASSERT(archiveQueue_);
//             std::lock_guard<std::mutex> lock(archiveQueuePtrMutex_);
//             archiveQueue_->close();
//         }
//         FDBStats stats = archiveFuture_.get();
//         ASSERT(!archiveQueue_);
//         archiveID_ = 0;

//         ASSERT(stats.numFlush() == 0);
//         size_t numArchive = stats.numArchive();

//         Buffer sendBuf(4096);
//         MemoryStream s(sendBuf);
//         s << numArchive;

//         // The flush call is blocking
//         controlWriteCheckResponse(fdb5::remote::Message::Flush, generateRequestID(), sendBuf, s.position());

//         internalStats_ += stats;
//     }

//     timer.stop();
//     internalStats_.addFlush(timer);
// }



FDBStats RemoteStore::archiveThreadLoop() {
    FDBStats localStats;
    eckit::Timer timer;

    // We can pop multiple elements off the archive queue simultaneously, if
    // configured
    std::pair<uint32_t, std::pair<Key, Buffer> > element;
    // for (size_t i = 0; i < maxArchiveBatchSize_; ++i) {
    //     elements.emplace_back(std::make_pair(Key{}, Buffer{0}));
    // }

    try {

        ASSERT(archiveQueue_);
//        ASSERT(archiveID_ != 0);

//        std::cout << "RemoteStore::archiveThreadLoop \n";
        long popped;
        while ((popped = archiveQueue_->pop(element)) != -1) {

            timer.start();
//        std::cout << "RemoteStore::archiveThreadLoop - sendArchiveData " <<  element.second.second.size() << std::endl;
            sendArchiveData(element.first, element.second.first, element.second.second.data(), element.second.second.size());
            timer.stop();

            localStats.addArchive(element.second.second.size(), timer, 1);
        }

        // And note that we are done. (don't time this, as already being blocked
        // on by the ::flush() routine)

       //MessageHeader hdr(Message::Flush, 0, 0, 0);
       dataWrite(Message::Flush, nullptr, 0);
//       dataWrite(id, &EndMarker, sizeof(EndMarker));

//        archiveID_ = 0;
        archiveQueue_.reset();

    } catch (...) {
        archiveQueue_->interrupt(std::current_exception());
        throw;
    }

    return localStats;

    // We are inside an async, so don't need to worry about exceptions escaping.
    // They will be released when flush() is called.
}


bool RemoteStore::handle(Message message, uint32_t requestID) {
//    std::cout << "RemoteStore::handle received message: " << ((uint) message) << " - requestID: " << requestID << std::endl;
    return false;

}
bool RemoteStore::handle(Message message, uint32_t requestID, eckit::net::Endpoint endpoint, eckit::Buffer&& payload) {

//    std::cout << "RemoteStore::handle received message: " << ((uint) message) << " - requestID: " << requestID << std::endl;
    if (message == Message::Archive) {
        auto it = locations_.find(requestID);
        if (it != locations_.end()) {
            MemoryStream s(payload);
            std::unique_ptr<FieldLocation> location(eckit::Reanimator<FieldLocation>::reanimate(s));
            // std::cout <<  "RemoteStore::handle - " << location->uri().asRawString() << " " << location->length() << std::endl;
            std::unique_ptr<RemoteFieldLocation> remoteLocation = std::unique_ptr<RemoteFieldLocation>(new RemoteFieldLocation(endpoint, *location));

            it->second.set_value(std::move(remoteLocation));
        }
        return true;
    }
    return false;
}
void RemoteStore::handleException(std::exception_ptr e) {
    std::cout << "RemoteStore::handleException" << std::endl;
}


void RemoteStore::flush(FDBStats& internalStats) {

    std::cout << "###################################      RemoteStore::flush\n";
    // Flush only does anything if there is an ongoing archive();
    if (! archiveFuture_.valid()) return;

//    ASSERT(archiveID_ != 0);
    {
        ASSERT(archiveQueue_);
        std::lock_guard<std::mutex> lock(archiveQueuePtrMutex_);
        archiveQueue_->close();
    }
    FDBStats stats = archiveFuture_.get();
    ASSERT(!archiveQueue_);
//    archiveID_ = 0;

    ASSERT(stats.numFlush() == 0);
    size_t numArchive = stats.numArchive();

    Buffer sendBuf(4096);
    MemoryStream s(sendBuf);
    s << numArchive;

    // The flush call is blocking
    controlWriteCheckResponse(Message::Flush, sendBuf, s.position());

    internalStats += stats;
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

namespace {

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

        RemoteStore::StoredMessage msg = std::make_pair(remote::MessageHeader{}, eckit::Buffer{0});
        ASSERT(queue_.pop(msg) != -1);

        // TODO; Error handling in the retrieve pathway

        const MessageHeader& hdr(msg.first);

        ASSERT(hdr.marker == StartMarker);
        ASSERT(hdr.version == CurrentVersion);

        // Handle any remote errors communicated from the server

        if (hdr.message == fdb5::remote::Message::Error) {
            std::string errmsg(static_cast<const char*>(msg.second), msg.second.size());
            throw DecoupledFDBException(errmsg, remoteEndpoint_);
        }

        // Are we now complete

        if (hdr.message == fdb5::remote::Message::Complete) {
            complete_ = 0;
            return 0;
        }

        ASSERT(hdr.message == fdb5::remote::Message::Blob);

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

}

// // Here we do (asynchronous) retrieving related stuff

// //DataHandle* RemoteStore::retrieve(const metkit::mars::MarsRequest& request) {

// //    connect();

// //    Buffer encodeBuffer(4096);
// //    MemoryStream s(encodeBuffer);
// //    s << request;

// //    uint32_t id = generateRequestID();

// //    controlWriteCheckResponse(fdb5::remote::Message::Retrieve, id, encodeBuffer, s.position());

// //    return new FDBRemoteDataHandle(id, retrieveMessageQueue_, controlEndpoint_);
// //}


// // Here we do (asynchronous) read related stuff


// eckit::DataHandle* RemoteStore::dataHandle(const FieldLocation& fieldLocation) {
//     return dataHandle(fieldLocation, Key());
// }

// eckit::DataHandle* RemoteStore::dataHandle(const FieldLocation& fieldLocation, const Key& remapKey) {

//     connect();

//     Buffer encodeBuffer(4096);
//     MemoryStream s(encodeBuffer);
//     s << fieldLocation;
//     s << remapKey;

//     uint32_t id = generateRequestID();

//     controlWriteCheckResponse(fdb5::remote::Message::Read, id, encodeBuffer, s.position());

//     return new FDBRemoteDataHandle(id, retrieveMessageQueue_, controlEndpoint_);
// }



// long RemoteStore::sendArchiveData(uint32_t id, const std::vector<std::pair<Key, Buffer>>& elements, size_t count) {
//     if (count == 1) {
//         sendArchiveData(id, elements[0].first, elements[0].second.data(), elements[0].second.size());
//         return elements[0].second.size();
//     }

//     // Serialise the keys

//     std::vector<Buffer> keyBuffers;
//     std::vector<size_t> keySizes;
//     keyBuffers.reserve(count);
//     keySizes.reserve(count);

//     size_t containedSize = 0;

//     for (size_t i = 0; i < count; ++i) {
//         keyBuffers.emplace_back(Buffer {4096});
//         MemoryStream keyStream(keyBuffers.back());
//         keyStream << elements[i].first;
//         keySizes.push_back(keyStream.position());
//         containedSize += (keyStream.position() + elements[i].second.size() +
//                           sizeof(MessageHeader) + sizeof(EndMarker));
//     }

//     // Construct the containing message

//     MessageHeader message(Message::MultiBlob, id, containedSize);
//     dataWrite(&message, sizeof(message));

//     long dataSent = 0;

//     for (size_t i = 0; i < count; ++i) {
//         MessageHeader containedMessage(Message::Blob, id, elements[i].second.size() + keySizes[i]);
//         dataWrite(&containedMessage, sizeof(containedMessage));
//         dataWrite(keyBuffers[i], keySizes[i]);
//         dataWrite(elements[i].second.data(), elements[i].second.size());
//         dataWrite(&EndMarker, sizeof(EndMarker));
//         dataSent += elements[i].second.size();
//     }

//     dataWrite(&EndMarker, sizeof(EndMarker));
//     return dataSent;
// }


void RemoteStore::sendArchiveData(uint32_t id, const Key& key, const void* data, size_t length) {
    
    ASSERT(data);
    ASSERT(length != 0);

    Buffer keyBuffer(4096);
    MemoryStream keyStream(keyBuffer);

    keyStream << dbKey_;
    keyStream << key;

    MessageHeader message(Message::Blob, 0, id, length + keyStream.position());
    dataWrite(id, &message, sizeof(message));
    dataWrite(id, keyBuffer, keyStream.position());
    dataWrite(id, data, length);
    dataWrite(id, &EndMarker, sizeof(EndMarker));
}



eckit::DataHandle* RemoteStore::dataHandle(const FieldLocation& fieldLocation) {
    return dataHandle(fieldLocation, Key());
}

eckit::DataHandle* RemoteStore::dataHandle(const FieldLocation& fieldLocation, const Key& remapKey) {

    //connect();

    Buffer encodeBuffer(4096);
    MemoryStream s(encodeBuffer);
    s << fieldLocation;
    s << remapKey;

//    uint32_t id = generateRequestID();

    uint32_t id = controlWriteCheckResponse(fdb5::remote::Message::Read, encodeBuffer, s.position());

    return new FDBRemoteDataHandle(id, retrieveMessageQueue_, eckit::net::Endpoint("") /* controlEndpoint() */);
}




static StoreBuilder<RemoteStore> builder("fdbremote");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5::remote
