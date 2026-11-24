// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   RemoteStore.h
/// @author Emanuele Danovaro
/// @date   October 2023

#pragma once

#include "fdb5/api/helpers/MoveIterator.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/Store.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/remote/client/Client.h"
#include "fdb5/remote/client/RequestQueues.h"

#include "eckit/config/Configuration.h"
#include "eckit/container/Queue.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/Buffer.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/Length.h"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace fdb5::remote {

class Locations {

public:

    using Callback = std::function<void(const std::unique_ptr<const FieldLocation> fieldLocation)>;

    Locations() = default;

    Locations(const Locations&) = delete;
    Locations& operator=(const Locations&) = delete;

    /// Stops forwarding: the locations not forwarded yet are dropped (the archival is over, or abandoned)
    ~Locations() {
        {
            std::lock_guard<std::mutex> lock(locationMutex_);
            stopping_ = true;
        }
        forwardCondition_.notify_all();
        if (forwarder_.joinable()) {
            forwarder_.join();
        }
    }

    std::mutex& mutex() { return locationMutex_; }

    size_t archived() const { return fieldsArchived_; }

    // Returns true exactly once per cycle (until reset()), for the first archival request.
    // Deliberately lock-free to avoid deadlock against the listening thread.
    bool markStoreRequested() { return !storeRequested_.exchange(true); }

    void archive(uint32_t requestID, Callback catalogue_archive) {
        std::lock_guard<std::mutex> lock(locationMutex_);
        if (error_) {
            std::rethrow_exception(error_);
        }
        if (connectionError_) {
            std::rethrow_exception(connectionError_);
        }
        locations_[requestID] = std::move(catalogue_archive);
        fieldsArchived_++;
        checkCounts();
    }

    /// A location received from the server, for a pending archive request. Only queued: it is forwarded to the
    /// catalogue (by its callback) by another thread, as the caller is the listener of the connection to the store,
    /// which must not wait for another connection (the catalogue may be slow, or not answer at all).
    /// @returns false if requestID is not a pending archive request
    bool location(uint32_t requestID, std::unique_ptr<FieldLocation> location) {
        std::lock_guard<std::mutex> lock(locationMutex_);
        auto it = locations_.find(requestID);
        if (it == locations_.end()) {
            return false;
        }

        forwards_.emplace_back(std::move(it->second), std::move(location));
        locations_.erase(it);
        if (!forwarder_.joinable()) {
            forwarder_ = std::thread([this] { forwardLoop(); });
        }
        forwardCondition_.notify_all();
        return true;
    }

    /// True if nothing more is expected: either all locations have been received (and forwarded), or the archival
    /// failed. Never throws, so it is safe to use in destructors.
    bool finished() {
        std::lock_guard<std::mutex> lock(locationMutex_);
        return error_ || fieldsArchived_ == locationsReceived_;
    }

    /// Atomically checks for a failure and for completion.
    /// @throws the recorded failure, if any: a failed archival is never reported as complete
    /// @returns true if all the locations have been received (and forwarded), otherwise false: wait() then blocks
    ///          until they are
    bool complete() {
        std::lock_guard<std::mutex> lock(locationMutex_);

        if (error_) {
            std::rethrow_exception(error_);
        }

        checkCounts();
        if (fieldsArchived_ == locationsReceived_) {
            return true;
        }

        promiseArchivalCompleted_ = std::promise<size_t>{};
        archivalCompleted_ = promiseArchivalCompleted_.get_future();
        return false;
    }

    /// Records a server-side failure of a pending archive request and unblocks wait().
    /// @returns false if requestID is not a pending archive request
    bool fail(uint32_t requestID, const std::exception_ptr& eptr) {
        std::lock_guard<std::mutex> lock(locationMutex_);
        if (locations_.find(requestID) == locations_.end()) {
            return false;
        }
        setError(eptr);
        return true;
    }

    /// Records a lost connection and fails the archival if it is not over: locations still expected, or received but
    /// not forwarded to the catalogue yet (queued, or being forwarded, e.g. to a slow catalogue). In the latter case
    /// the flush would fail anyway (the store cannot be flushed any more): it is not left waiting for the catalogue.
    void failPending(const std::exception_ptr& eptr) {
        std::lock_guard<std::mutex> lock(locationMutex_);
        if (!connectionError_) {
            connectionError_ = eptr;
        }
        if (!locations_.empty() || !forwards_.empty() || forwarding_ > 0) {
            setError(eptr);
        }
    }

    size_t wait() {
        archivalCompleted_.wait();
        return archivalCompleted_.get();
    }

    void reset() {
        std::lock_guard<std::mutex> lock(locationMutex_);
        checkCounts();
        fieldsArchived_ = locations_.size() + forwards_.size() + forwarding_;
        locationsReceived_ = 0;
        storeRequested_ = false;
        error_ = nullptr;
    }

private:

    // Forwards the received locations to the catalogue, in order, without the lock
    void forwardLoop() {
        std::unique_lock<std::mutex> lock(locationMutex_);
        while (true) {
            forwardCondition_.wait(lock, [this] { return stopping_ || !forwards_.empty(); });
            if (stopping_) {
                return;
            }

            auto [callback, location] = std::move(forwards_.front());
            forwards_.pop_front();
            ++forwarding_;
            lock.unlock();

            std::exception_ptr failure;
            try {
                callback(std::move(location));
            }
            catch (...) {
                // e.g. the catalogue failed: the archival fails, flush() reports it
                failure = std::current_exception();
            }

            lock.lock();
            --forwarding_;
            locationsReceived_++;
            if (failure) {
                setError(failure);
            }
            checkCounts();
            // after a failure the promise has already been satisfied with the error
            if (!error_ && archivalCompleted_.valid() && (fieldsArchived_ == locationsReceived_)) {
                promiseArchivalCompleted_.set_value(locationsReceived_);
            }
        }
    }

    // caller must hold locationMutex_: every archived field is pending, being forwarded, or received
    void checkCounts() const {
        ASSERT(fieldsArchived_ - locationsReceived_ == locations_.size() + forwards_.size() + forwarding_);
    }

    // caller must hold locationMutex_
    void setError(const std::exception_ptr& eptr) {
        if (error_) {
            return;
        }
        error_ = eptr;
        if (archivalCompleted_.valid()) {
            try {
                promiseArchivalCompleted_.set_exception(eptr);
            }
            catch (const std::future_error&) {
                // already satisfied
            }
        }
    }

    std::mutex locationMutex_;
    std::exception_ptr error_;
    std::exception_ptr connectionError_;
    std::map<uint32_t, Callback> locations_;  // archive requests waiting for their location
    std::deque<std::pair<Callback, std::unique_ptr<FieldLocation>>> forwards_;  // locations to forward
    size_t forwarding_{0};                                                      // locations being forwarded
    size_t fieldsArchived_{0};
    size_t locationsReceived_{0};
    std::promise<size_t> promiseArchivalCompleted_;
    std::future<size_t> archivalCompleted_;
    std::atomic<bool> storeRequested_{false};

    std::condition_variable forwardCondition_;
    bool stopping_{false};
    std::thread forwarder_;  // n.b. last: joined first
};

//----------------------------------------------------------------------------------------------------------------------

/// Store that connects to a remote Store
class RemoteStore : public Store, public Client {

public:  // types

    using StoredMessage = std::pair<Message, eckit::Buffer>;
    using MessageQueue = RequestQueues<StoredMessage>::Queue;

public:  // methods

    RemoteStore(const Key& key, const Config& config);
    RemoteStore(const eckit::URI& uri, const Config& config);

    ~RemoteStore() override;

    static RemoteStore& get(const eckit::URI& uri);

    static const char* typeName() { return "remote"; }
    eckit::URI uri() const override;
    static eckit::URI uri(const eckit::URI& dataURI);

    bool open() override;
    size_t flush() override;
    void close() override;

    void checkUID() const override {}

    eckit::DataHandle* dataHandle(const FieldLocation& fieldLocation);
    eckit::DataHandle* dataHandle(const FieldLocation& fieldLocation, const Key& remapKey);

    /// A read request that could not be sent fails: its reader gets the reason (see ReadLimiter)
    void failRequest(uint32_t requestID, const std::exception_ptr& reason);

    bool canMoveTo(const Key& key, const Config& config, const eckit::URI& dest) const override { return false; }
    void moveTo(const Key& key, const Config& config, const eckit::URI& dest,
                eckit::Queue<MoveElement>& queue) const override {
        NOTIMP;
    }
    bool uriBelongs(const eckit::URI&) const override;
    bool uriExists(const eckit::URI&) const override;
    std::set<eckit::URI> collocatedDataURIs() const override;
    std::set<eckit::URI> asCollocatedDataURIs(const std::set<eckit::URI>&) const override;

    std::vector<eckit::URI> getAuxiliaryURIs(const eckit::URI&, bool onlyExisting = false) const override;

    void finaliseWipeState(StoreWipeState& storeState, bool doit, bool unsafeWipeAll) override;
    bool doWipeUnknowns(const std::set<eckit::URI>& unknownURIs) const override;
    bool doWipeURIs(const StoreWipeState& wipeState) const override;
    void doWipeEmptyDatabase() const override;
    bool doUnsafeFullWipe() const override;

    const Config& config() const { return config_; }

protected:  // methods

    std::string type() const override { return typeName(); }

    bool exists() const override;

    eckit::DataHandle* retrieve(Field& field) const override;
    void archiveCb(
        const Key& key, const void* data, eckit::Length length,
        std::function<void(const std::unique_ptr<const FieldLocation> fieldLocation)> catalogue_archive) override;

    void remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const override;

    void print(std::ostream& out) const override;

private:  // methods

    const eckit::Configuration& clientConfig() const override;
    // handlers for incoming messages - to be defined in the client class
    bool handle(Message message, uint32_t requestID) override;
    bool handle(Message message, uint32_t requestID, eckit::Buffer&& payload) override;
    void closeConnection(const std::exception_ptr& reason) override;

private:  // members

    // used by tests for direct access to requests_
    friend class RemoteStoreTestHarness;

    Key dbKey_;

    const Config config_;  // a copy: get() is given a temporary

    // The queues of the read requests (see RequestQueues for their lifetime)
    RequestQueues<StoredMessage> requests_;

    Locations locations_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
