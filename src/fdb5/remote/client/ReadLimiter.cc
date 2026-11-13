// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/remote/client/ReadLimiter.h"

#include "fdb5/remote/client/RemoteStore.h"

#include "eckit/config/Resource.h"

#include <mutex>
#include <sstream>

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------
namespace {
std::mutex instanceMutex_;
std::unique_ptr<ReadLimiter> instance_{nullptr};
}  // namespace


ReadLimiter& ReadLimiter::instance() {
    // the instance cannot be a static ReadLimiter, which is causing the following error on exit,
    // when the instance is destroyed and the mutex is destroyed before the instance:
    // libc++abi: terminating due to uncaught exception of type std::__1::system_error: mutex lock failed: Invalid
    // argument
    std::lock_guard<std::mutex> lock(instanceMutex_);
    if (instance_ == nullptr) {
        instance_.reset(new ReadLimiter(defaultReadLimit()));
    }
    return *instance_;
}

size_t ReadLimiter::defaultReadLimit() {
    static size_t limit = eckit::Resource<size_t>("$FDB_READ_LIMIT;fdbReadLimit", size_t{1_GiB});  // 1 GiB default
    return limit;
}

void ReadLimiter::setMemoryLimit(size_t memoryLimit) {
    {
        std::lock_guard lock(mutex_);
        memoryLimit_ = memoryLimit;
    }
    tryNextRequest();
}

ReadLimiter::ReadLimiter(size_t memoryLimit) : memoryUsed_{0}, memoryLimit_{memoryLimit} {}

void ReadLimiter::add(RemoteStore* client, uint32_t id, const FieldLocation& fieldLocation, const Key& remapKey) {
    eckit::Buffer requestBuffer(4096);
    eckit::MemoryStream s(requestBuffer);
    s << fieldLocation;
    s << remapKey;
    size_t requestSize = s.position();
    size_t resultSize = fieldLocation.length();

    if (resultSize > memoryLimit_) {
        std::ostringstream ss;
        ss << "ReadLimiter: Requested field size " << resultSize << " exceeds memory limit " << memoryLimit_
           << ". Field: " << fieldLocation.fullUri();
        throw eckit::SeriousBug(ss.str());
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        requests_.emplace_back(RequestInfo{client, id, std::move(requestBuffer), requestSize, resultSize});
    }

    tryNextRequest();
}

bool ReadLimiter::tryNextRequest() {
    while (true) {
        RequestInfo request{nullptr, 0, eckit::Buffer{0}, 0, 0};
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (requests_.empty()) {
                return false;
            }
            if (memoryUsed_ + requests_.front().resultSize > memoryLimit_) {
                return false;
            }
            request = std::move(requests_.front());
            requests_.pop_front();
            activeRequests_[request.client->id()].insert(request.id);
            resultSizes_[{request.client->id(), request.id}] = request.resultSize;
            memoryUsed_ += request.resultSize;
        }
        // Send outside the lock — sendRequest may block on network I/O
        try {
            sendRequest(request);
            return true;
        }
        catch (...) {
            // e.g. the store's connection was lost. This may run in another request's finishRequest() (even in a
            // DataHandle destructor), so do not throw: give the memory back and pass the error to the request's
            // own reader. Then try the next request, which would otherwise wait for an unrelated event.
            const auto error = std::current_exception();
            {
                std::lock_guard<std::mutex> lock(mutex_);
                releaseActive(request.client->id(), request.id);
            }
            request.client->failRequest(request.id, error);
        }
    }
}

bool ReadLimiter::releaseActive(uint32_t clientID, uint32_t requestID) {
    auto it = activeRequests_.find(clientID);
    if (it == activeRequests_.end() || it->second.erase(requestID) == 0) {
        return false;
    }
    if (it->second.empty()) {
        activeRequests_.erase(it);
    }
    memoryUsed_ -= resultSizes_[{clientID, requestID}];
    resultSizes_.erase({clientID, requestID});
    return true;
}

void ReadLimiter::finishRequest(uint32_t clientID, uint32_t requestID) {
    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!releaseActive(clientID, requestID)) {
            // Not sent (yet): its DataHandle was dropped while the request was queued, e.g. after its reader was
            // woken by a lost connection. Do not send it any more. If it already failed, there is nothing to do.
            for (auto r = requests_.begin(); r != requests_.end(); ++r) {
                if (r->client->id() == clientID && r->id == requestID) {
                    requests_.erase(r);
                    break;
                }
            }
            return;
        }
    }

    tryNextRequest();
}

/// @note: Called when a RemoteStore is destroyed (on exit), and when RemoteStore::get() replaces a read store that lost
/// its connection.
void ReadLimiter::evictClient(size_t clientID) {
    {
        std::lock_guard<std::mutex> lock(instanceMutex_);
        if (instance_ == nullptr) {
            return;
        }
    }
    // Instance is never destroyed once created, so safe to access without instanceMutex_
    auto& limiter = instance();
    {
        std::lock_guard<std::mutex> lock(limiter.mutex_);
        // Remove the client's active requests
        auto it = limiter.activeRequests_.find(clientID);

        if (it != limiter.activeRequests_.end()) {
            const auto requestIDs = it->second;  // copy: releaseActive() erases from the set
            for (auto requestID : requestIDs) {
                limiter.releaseActive(clientID, requestID);
            }
        }

        // Clean up any pending requests attributed to this client
        ///@note O(n), room for optimisation.
        auto it2 = limiter.requests_.begin();
        while (it2 != limiter.requests_.end()) {
            if (it2->client->id() == clientID) {
                it2 = limiter.requests_.erase(it2);
            }
            else {
                ++it2;  // Only increment if we didn't erase
            }
        }
    }
    // mutex_ released; tryNextRequest() will re-acquire it (and release before sendRequest)
    limiter.tryNextRequest();
}

void ReadLimiter::print(std::ostream& out) const {
    std::lock_guard<std::mutex> lock(mutex_);

    out << "ReadLimiter(memoryUsed=" << memoryUsed_ << ", memoryLimit=" << memoryLimit_ << ") {" << std::endl;

    out << "  Queued Requests: ";
    for (const auto& request : requests_) {
        out << request.id << " ";
    }
    out << std::endl;

    out << "  Active Requests: ";
    for (const auto& [reqid, size] : activeRequests_) {
        out << reqid << " ";
    }
    out << "}" << std::endl;
}

void ReadLimiter::sendRequest(const RequestInfo& request) const {
    request.client->controlWriteCheckResponse(Message::Read, request.id, true, request.requestBuffer,
                                              request.requestSize);
}

}  // namespace fdb5::remote
