/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/remote/client/RemoteStore.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Store.h"
#include "fdb5/remote/Connection.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/remote/RemoteFieldLocation.h"
#include "fdb5/remote/client/Client.h"
#include "fdb5/rules/Rule.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"
#include "eckit/log/Log.h"
#include "eckit/net/Endpoint.h"
#include "eckit/runtime/Main.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/serialisation/Reanimator.h"

#include <dirent.h>
#include <fcntl.h>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <set>
#include <string>
#include <utility>
#include <vector>

using namespace eckit;

//----------------------------------------------------------------------------------------------------------------------

namespace fdb5::remote {

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

    FDBRemoteDataHandle(uint32_t requestID, Length estimate,
                        std::shared_ptr<RemoteStore::MessageQueue> queue,
                        const net::Endpoint& remoteEndpoint) :
        requestID_(requestID),
        estimate_(estimate),
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

        long total = 0;
        long n;
        char* p    = static_cast<char*>(pos);
        if (currentBuffer_.size() != 0) {
            n = bufferRead(pos, sz);
            sz -= n;
            total += n;
            p+=n;
        }

        while (sz > 0 && !complete_) {

            // If we are in the DataHandle, then there MUST be data to read
            RemoteStore::StoredMessage msg = std::make_pair(remote::Message{}, eckit::Buffer{0});
            ASSERT(queue_->pop(msg) != -1);

            // Handle any remote errors communicated from the server
            if (msg.first == Message::Error) {
                std::string errmsg(static_cast<const char*>(msg.second.data()), msg.second.size());
                throw RemoteFDBException(errmsg, remoteEndpoint_);
            }

            // Are we now complete?
            if (msg.first == Message::Complete) {
                if (overallPosition_ == eckit::Offset(0)) {
                    ASSERT(queue_->pop(msg) != -1);
                } else {
                    complete_ = true;
                    return total;
                }
            }

            ASSERT(msg.first == Message::Blob);

            // Otherwise return the data!
            std::swap(currentBuffer_, msg.second);

            n = bufferRead(p, sz);
            sz -= n;
            total += n;
            p+=n;

        }
        return total;
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
        return estimate_;
    }

    Offset position() override {
        return overallPosition_;
    }

private: // members

    uint32_t requestID_;
    Length estimate_;
    std::shared_ptr<RemoteStore::MessageQueue> queue_;
    net::Endpoint remoteEndpoint_;
    size_t pos_;
    Offset overallPosition_;
    Buffer currentBuffer_;
    bool complete_;
};

std::vector<std::pair<eckit::net::Endpoint, std::string>> storeEndpoints(const Config& config) {

    ASSERT(config.has("stores"));
    ASSERT(config.has("fieldLocationEndpoints"));
    std::vector<std::string> stores = config.getStringVector("stores");
    std::vector<std::string> fieldLocationEndpoints = config.getStringVector("fieldLocationEndpoints");

    ASSERT(stores.size() == fieldLocationEndpoints.size());
    std::vector<std::pair<eckit::net::Endpoint, std::string>> out;
    for (size_t i=0; i<stores.size(); i++) {
        out.push_back(std::make_pair(eckit::net::Endpoint{stores.at(i)}, fieldLocationEndpoints.at(i)));
    }
    return out;
}

//----------------------------------------------------------------------------------------------------------------------

RemoteStore::RemoteStore(const Key& dbKey, const Config& config) :
    Client(storeEndpoints(config)),
    dbKey_(dbKey), config_(config)
    {}

// this is used only in retrieval, with an URI already referring to an accessible Store
RemoteStore::RemoteStore(const eckit::URI& uri, const Config& config) :
    Client(eckit::net::Endpoint(uri.hostport()), uri.hostport()),
    dbKey_(Key()), config_(config)
    {
    // no need to set the local_ flag on the read path
    ASSERT(uri.scheme() == "fdb");
}

RemoteStore::~RemoteStore() {
    // If we have launched a thread with an async and we manage to get here, this is
    // an error. n.b. if we don't do something, we will block in the destructor
    // of std::future.
    if (!locations_.complete()) {
        Log::error() << "Attempting to destruct RemoteStore with active archival" << std::endl;
        eckit::Main::instance().terminate();
    }
}

eckit::URI RemoteStore::uri() const {
    return URI("fdb", "");
}

bool RemoteStore::exists() const {
    return true;
}

eckit::DataHandle* RemoteStore::retrieve(Field& field) const {
    return field.dataHandle();
}

void RemoteStore::archive(const Key& key, const void *data, eckit::Length length, std::function<void(const std::unique_ptr<const FieldLocation> fieldLocation)> catalogue_archive) {

    ASSERT(!key.empty());
    ASSERT(data);
    ASSERT(length != 0);

    uint32_t id = generateRequestID();
    {   // send the archival request
        std::lock_guard<std::mutex> lock(locations_.mutex());
        if (locations_.archived() == 0) { // if this is the first archival request, notify the server
            controlWriteCheckResponse(Message::Store, id, true);
        }
    }
    // store the callback, associated with the request id - to be done BEFORE sending the data
    locations_.archive(id, catalogue_archive);

    Buffer keyBuffer(4096);
    MemoryStream keyStream(keyBuffer);
    keyStream << dbKey_;
    keyStream << key;

    PayloadList payloads;
    payloads.emplace_back(keyStream.position(), keyBuffer.data());
    payloads.emplace_back(length, data);

    dataWrite(Message::Blob, id, payloads);

    eckit::Log::status() << "Field " << locations_.archived() << " enqueued for store archival" << std::endl;
}

bool RemoteStore::open() {
    return true;
}

size_t RemoteStore::flush() {

    // Flush only does anything if there is an ongoing archive();
    if (locations_.archived() == 0) {
        return 0;
    }

    // remote side reported that all fields are flushed... now wait for field locations
    // size_t locations;
    bool complete = locations_.complete();

    size_t locations = complete ? locations_.archived() : locations_.wait();

    Buffer sendBuf(1024);
    MemoryStream s(sendBuf);
    s << locations;

    LOG_DEBUG_LIB(LibFdb5) << " RemoteStore::flush - flushing " << locations << " fields" << std::endl;
    // The flush call is blocking
    controlWriteCheckResponse(Message::Flush, generateRequestID(), false, sendBuf, s.position());

    locations_.reset();

    return locations;
}

void RemoteStore::close() {
}

void RemoteStore::remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const {
    NOTIMP;
}

void RemoteStore::remove(const Key& key) const {
    NOTIMP;
}

void RemoteStore::print(std::ostream &out) const {
    out << "RemoteStore(host=" << controlEndpoint() << ")";
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
                std::lock_guard<std::mutex> lock(retrieveMessageMutex_);
                auto id = retrieveMessageQueues_.find(requestID);
                ASSERT (id != retrieveMessageQueues_.end());

                id->second->emplace(std::make_pair(message, Buffer(0)));

                retrieveMessageQueues_.erase(id);
            }
            return true;
        }
        case Message::Error: {

            auto it = messageQueues_.find(requestID);
            if (it != messageQueues_.end()) {
                it->second->interrupt(std::make_exception_ptr(RemoteFDBException("", controlEndpoint())));

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

        case Message::Store: { // received a Field location from the remote store, can forward to the archiver for the indexing
            MemoryStream s(payload);
            std::unique_ptr<FieldLocation> location(eckit::Reanimator<FieldLocation>::reanimate(s));
            if (defaultEndpoint().empty()) {
                return locations_.location(requestID, std::move(location));
            } else {
                std::unique_ptr<RemoteFieldLocation> remoteLocation = std::unique_ptr<RemoteFieldLocation>(new RemoteFieldLocation(eckit::net::Endpoint{defaultEndpoint()}, *location));
                return locations_.location(requestID, std::move(remoteLocation));
            }
        }
        case Message::Blob: {
            auto it = messageQueues_.find(requestID);
            if (it != messageQueues_.end()) {
                it->second->emplace(message, std::move(payload));
            } else {
                std::lock_guard<std::mutex> lock(retrieveMessageMutex_);
                auto id = retrieveMessageQueues_.find(requestID);
                ASSERT (id != retrieveMessageQueues_.end());
                id->second->emplace(std::make_pair(message, std::move(payload)));
            }
            return true;
        }
        case Message::Error: {

            auto it = messageQueues_.find(requestID);
            if (it != messageQueues_.end()) {
                std::string msg;
                msg.resize(payload.size(), ' ');
                payload.copy(&msg[0], payload.size());
                it->second->interrupt(std::make_exception_ptr(RemoteFDBException(msg, controlEndpoint())));

                // Remove entry (shared_ptr --> message queue will be destroyed when it
                // goes out of scope in the worker thread).
                messageQueues_.erase(it);

            }
            return true;
        }
        default:
            return false;
    }
}

eckit::DataHandle* RemoteStore::dataHandle(const FieldLocation& fieldLocation) {
    return dataHandle(fieldLocation, Key());
}

eckit::DataHandle* RemoteStore::dataHandle(const FieldLocation& fieldLocation, const Key& remapKey) {

    Buffer encodeBuffer(4096);
    MemoryStream s(encodeBuffer);
    s << fieldLocation;
    s << remapKey;

    uint32_t id = generateRequestID();

    static size_t queueSize = 320;
    std::shared_ptr<MessageQueue> queue = nullptr;
    {
        std::lock_guard<std::mutex> lock(retrieveMessageMutex_);

        auto entry = retrieveMessageQueues_.emplace(id, std::make_shared<MessageQueue>(queueSize));
        ASSERT(entry.second);

        queue = entry.first->second;
    }

    controlWriteCheckResponse(fdb5::remote::Message::Read, id, true, encodeBuffer, s.position());

    return new FDBRemoteDataHandle(id, fieldLocation.length(), queue, controlEndpoint());
}

RemoteStore& RemoteStore::get(const eckit::URI& uri) {
    static std::mutex storeMutex_;
    // we memoise one read store for each endpoint. Do not need to have one for each key
    static std::map<std::string, std::unique_ptr<RemoteStore>> readStores_;

    std::lock_guard<std::mutex> lock(storeMutex_);
    const std::string& endpoint = uri.hostport();
    auto it = readStores_.find(endpoint);
    if (it != readStores_.end()) {
        return *(it->second);
    }

    return *(readStores_[endpoint] = std::unique_ptr<RemoteStore>(new RemoteStore(uri, Config())));
}

    bool RemoteStore::uriBelongs(const eckit::URI&) const {
        NOTIMP;
    }
    bool RemoteStore::uriExists(const eckit::URI&) const {
        NOTIMP;
    }
    std::vector<eckit::URI> RemoteStore::collocatedDataURIs() const {
        NOTIMP;
    }
    std::set<eckit::URI> RemoteStore::asCollocatedDataURIs(const std::vector<eckit::URI>&) const {
        NOTIMP;
    }

static StoreBuilder<RemoteStore> builder("remote");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5::remote
