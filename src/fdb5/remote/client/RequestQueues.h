// SPDX-FileCopyrightText: 2026- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "eckit/container/Queue.h"
#include "eckit/exception/Exceptions.h"

#include <cstddef>
#include <cstdint>
#include <exception>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

/// The queues of the requests of a client whose answer is streamed (reads, list, axes, ...), by requestID.
///
/// Lifetime of a request:
///  - add() registers it before it is sent. The Registration unregisters it again if it is destroyed before release():
///    a request that could not be sent never stays registered.
///  - the listener queues its messages with enqueue(), and ends it with complete() (last message) or fail() (error);
///    both unregister it.
///  - failAll() fails and unregisters every request: the connection is lost, nothing more will arrive.
///  - the consumer may abandon it with cancel(): its queue is interrupted, so that the messages still to come are
///    discarded, but it stays registered until the server ends it (otherwise they could not be told apart from
///    messages for an unknown request).
///
/// A message is never queued with the mutex held: queueing blocks while the queue is full (the consumer is slower),
/// and failAll()/fail() must still be able to interrupt it.
template <typename Element>
class RequestQueues {
public:  // types

    using Queue = eckit::Queue<Element>;

    class Registration {
    public:

        Registration(RequestQueues& owner, uint32_t requestID, std::shared_ptr<Queue> queue) :
            owner_(&owner), requestID_(requestID), queue_(std::move(queue)) {}

        Registration(Registration&& other) noexcept :
            owner_(std::exchange(other.owner_, nullptr)), requestID_(other.requestID_), queue_(std::move(other.queue_)) {}

        Registration(const Registration&) = delete;
        Registration& operator=(const Registration&) = delete;
        Registration& operator=(Registration&&) = delete;

        ~Registration() {
            if (owner_) {
                owner_->unregister(requestID_, queue_.get());
            }
        }

        const std::shared_ptr<Queue>& queue() const { return queue_; }

        /// The request has been sent: from now on, it is unregistered by its last message, or by a failure
        std::shared_ptr<Queue> release() {
            owner_ = nullptr;
            return queue_;
        }

    private:

        RequestQueues* owner_;
        uint32_t requestID_;
        std::shared_ptr<Queue> queue_;
    };

public:  // methods

    /// @throws SeriousBug if the requestID is already registered
    Registration add(uint32_t requestID, size_t capacity) {
        auto queue = std::make_shared<Queue>(capacity);
        std::lock_guard<std::mutex> lock(mutex_);
        if (!queues_.emplace(requestID, queue).second) {
            throw eckit::SeriousBug("Request " + std::to_string(requestID) + " is already registered", Here());
        }
        return Registration{*this, requestID, std::move(queue)};
    }

    std::shared_ptr<Queue> find(uint32_t requestID) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = queues_.find(requestID);
        return it == queues_.end() ? nullptr : it->second;
    }

    /// Queues a message for a request, waiting while its queue is full. A message for a cancelled request is
    /// discarded: nobody will read it, and it is not a failure of the connection.
    /// @returns false if the request is not registered
    bool enqueue(uint32_t requestID, Element&& element) {
        auto queue = find(requestID);
        if (!queue) {
            return false;
        }
        push(*queue, std::move(element));
        return true;
    }

    /// Ends a request: finish(Queue&) delivers its last message (e.g. queues it, or closes the queue), then it is
    /// unregistered. It stays registered meanwhile, so that failAll() can interrupt a blocked finish().
    /// @returns false if the request is not registered
    template <typename F>
    bool complete(uint32_t requestID, F&& finish) {
        auto queue = find(requestID);
        if (!queue) {
            return false;
        }
        struct Unregister {
            RequestQueues& owner;
            uint32_t requestID;
            const Queue* queue;
            ~Unregister() { owner.unregister(requestID, queue); }
        } unregister{*this, requestID, queue.get()};

        try {
            finish(*queue);
        }
        catch (...) {
            if (!queue->closed()) {  // see push()
                throw;
            }
        }
        return true;
    }

    /// Fails a request: its consumer gets the reason
    /// @returns false if the request is not registered
    bool fail(uint32_t requestID, const std::exception_ptr& reason) {
        std::shared_ptr<Queue> queue;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = queues_.find(requestID);
            if (it == queues_.end()) {
                return false;
            }
            queue = std::move(it->second);
            queues_.erase(it);
        }
        interrupt(*queue, reason);
        return true;
    }

    /// Fails all the requests (e.g. the connection is lost): nothing more will arrive for them
    void failAll(const std::exception_ptr& reason) {
        std::map<uint32_t, std::shared_ptr<Queue>> queues;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            std::swap(queues, queues_);
        }
        for (auto& [id, queue] : queues) {
            interrupt(*queue, reason);
        }
    }

    /// The consumer abandons the request: the messages still to come are discarded (see class description)
    static void cancel(Queue& queue, const std::exception_ptr& reason) noexcept { interrupt(queue, reason); }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queues_.size();
    }

private:  // methods

    static void push(Queue& queue, Element&& element) {
        try {
            queue.emplace(std::move(element));
        }
        catch (...) {
            // interrupted (failed or cancelled) meanwhile: the message is discarded
            if (!queue.closed()) {
                throw;
            }
        }
    }

    static void interrupt(Queue& queue, const std::exception_ptr& reason) noexcept {
        try {
            if (!queue.closed()) {
                queue.interrupt(reason);
            }
        }
        catch (...) {
        }
    }

    // Unregisters the request, if it is still registered with this queue (requestIDs restart on a new connection)
    void unregister(uint32_t requestID, const Queue* queue) noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = queues_.find(requestID);
            if (it != queues_.end() && it->second.get() == queue) {
                queues_.erase(it);
            }
        }
        catch (...) {
        }
    }

private:  // members

    mutable std::mutex mutex_;
    std::map<uint32_t, std::shared_ptr<Queue>> queues_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
