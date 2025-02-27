/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   RemoteStore.h
/// @author Emanuele Danovaro
/// @date   October 2023

#pragma once

#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/Store.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/remote/client/Client.h"

#include <map>
#include <mutex>
#include <set>
#include <vector>

namespace fdb5::remote {

class Locations {

public:

    Locations() : fieldsArchived_(0), locationsReceived_(0) {}

    std::mutex& mutex() { return locationMutex_; }

    size_t archived() { return fieldsArchived_; }

    void archive(uint32_t requestID,
                 std::function<void(const std::unique_ptr<const FieldLocation> fieldLocation)> catalogue_archive) {
        std::lock_guard<std::mutex> lock(locationMutex_);
        locations_[requestID] = catalogue_archive;
        fieldsArchived_++;
        ASSERT(fieldsArchived_ - locationsReceived_ == locations_.size());
    }

    bool location(uint32_t requestID, std::unique_ptr<FieldLocation> location) {

        std::lock_guard<std::mutex> lock(locationMutex_);
        auto it = locations_.find(requestID);
        if (it == locations_.end()) {
            return false;
        }
        it->second(std::move(location));

        locations_.erase(it);
        locationsReceived_++;

        ASSERT(fieldsArchived_ - locationsReceived_ == locations_.size());
        if (archivalCompleted_.valid() && (fieldsArchived_ == locationsReceived_)) {
            promiseArchivalCompleted_.set_value(locationsReceived_);
        }
        return true;
    }

    bool complete() {
        std::lock_guard<std::mutex> lock(locationMutex_);

        ASSERT(fieldsArchived_ - locationsReceived_ == locations_.size());
        if (fieldsArchived_ == locationsReceived_)
            return true;

        promiseArchivalCompleted_ = std::promise<size_t>{};
        archivalCompleted_        = promiseArchivalCompleted_.get_future();
        return false;
    }

    size_t wait() {
        archivalCompleted_.wait();
        return archivalCompleted_.get();
    }

    void reset() {
        std::lock_guard<std::mutex> lock(locationMutex_);
        ASSERT(fieldsArchived_ - locationsReceived_ == locations_.size());
        fieldsArchived_    = locations_.size();
        locationsReceived_ = 0;
    }

private:

    std::mutex locationMutex_;
    std::map<uint32_t, std::function<void(const std::unique_ptr<const FieldLocation> fieldLocation)>> locations_;
    size_t fieldsArchived_;
    size_t locationsReceived_;
    std::promise<size_t> promiseArchivalCompleted_;
    std::future<size_t> archivalCompleted_;
};

//----------------------------------------------------------------------------------------------------------------------

/// Store that connects to a remote Store
class RemoteStore : public Store, public Client {

public:  // types

    using StoredMessage = std::pair<Message, eckit::Buffer>;
    using MessageQueue  = eckit::Queue<StoredMessage>;

    static const char* typeName() { return "remote"; }

public:  // methods

    RemoteStore(const Key& key, const Config& config);
    RemoteStore(const eckit::URI& uri, const Config& config);

    ~RemoteStore() override;

    static RemoteStore& get(const eckit::URI& uri);

    eckit::URI uri() const override;

    bool open() override;
    size_t flush() override;
    void close() override;

    void checkUID() const override {}

    eckit::DataHandle* dataHandle(const FieldLocation& fieldLocation);
    eckit::DataHandle* dataHandle(const FieldLocation& fieldLocation, const Key& remapKey);

    bool canMoveTo(const Key& key, const Config& config, const eckit::URI& dest) const override { return false; }
    void moveTo(const Key& key, const Config& config, const eckit::URI& dest,
                eckit::Queue<MoveElement>& queue) const override {
        NOTIMP;
    }
    void remove(const Key& key) const override;
    bool uriBelongs(const eckit::URI&) const override;
    bool uriExists(const eckit::URI&) const override;
    std::vector<eckit::URI> collocatedDataURIs() const override;
    std::set<eckit::URI> asCollocatedDataURIs(const std::vector<eckit::URI>&) const override;

    const Config& config() const { return config_; }

protected:  // methods

    std::string type() const override { return typeName(); }

    bool exists() const override;

    eckit::DataHandle* retrieve(Field& field) const override;
    void archive(
        const Key& key, const void* data, eckit::Length length,
        std::function<void(const std::unique_ptr<const FieldLocation> fieldLocation)> catalogue_archive) override;

    void remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const override;

    void print(std::ostream& out) const override;

private:  // methods

    // handlers for incoming messages - to be defined in the client class
    bool handle(Message message, uint32_t requestID) override;
    bool handle(Message message, uint32_t requestID, eckit::Buffer&& payload) override;
    void closeConnection() override;

private:  // members

    Key dbKey_;

    const Config& config_;

    // @note This is a map of requestID:MessageQueue. At the point that a request is
    // complete, errored or otherwise killed, it needs to be removed from the map.
    // The shared_ptr allows this removal to be asynchronous with the actual task
    // cleaning up and returning to the client.
    std::map<uint32_t, std::shared_ptr<MessageQueue>> messageQueues_;
    std::map<uint32_t, std::shared_ptr<MessageQueue>> retrieveMessageQueues_;

    std::mutex retrieveMessageMutex_;

    Locations locations_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
