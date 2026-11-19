// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/remote/client/ReadLimiter.h"

#include "eckit/log/Log.h"

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
    tryNextRequests();
}

ReadLimiter::ReadLimiter(size_t memoryLimit) : memoryUsed_{0}, memoryLimit_{memoryLimit} {}

void ReadLimiter::add(RemoteStore* client, uint32_t id, const FieldLocation& fieldLocation, const Key& remapKey) {
    eckit::Buffer requestBuffer(4096);
    eckit::MemoryStream s(requestBuffer);
    s << fieldLocation;
    s << remapKey;
    size_t requestSize = s.position();
    size_t resultSize = fieldLocation.length();

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (resultSize > memoryLimit_) {
            std::ostringstream ss;
            ss << "ReadLimiter: Requested field size " << resultSize << " exceeds memory limit " << memoryLimit_
               << ". Field: " << fieldLocation.fullUri();
            throw eckit::SeriousBug(ss.str());
        }
        requests_.emplace_back(RequestInfo{client, id, std::move(requestBuffer), requestSize, resultSize});
    }

    tryNextRequests();
}

void ReadLimiter::tryNextRequests() noexcept {
    while (true) {
        RequestInfo request{nullptr, 0, eckit::Buffer{0}, 0, 0};
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (requests_.empty()) {
                return;
            }
            if (memoryUsed_ + requests_.front().resultSize > memoryLimit_) {
                return;
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
        }
        catch (...) {
            // The request will never get an answer: release its budget, and give the error to its reader. The caller
            // (e.g. the destructor of another request's DataHandle) is not concerned.
            const auto eptr = std::current_exception();
            {
                std::lock_guard<std::mutex> lock(mutex_);
                releaseActive(request.client->id(), request.id);
            }
            try {
                request.client->failRequest(request.id, eptr);
            }
            catch (...) {
                eckit::Log::error() << "ReadLimiter: failed to report the failure of read request " << request.id
                                    << std::endl;
            }
        }
    }
}

bool ReadLimiter::releaseActive(uint32_t clientID, uint32_t requestID) {
    auto it = activeRequests_.find(clientID);
    if (it == activeRequests_.end()) {
        return false;
    }
    auto it2 = it->second.find(requestID);
    if (it2 == it->second.end()) {
        return false;
    }
    auto size = resultSizes_.find({clientID, requestID});
    if (size != resultSizes_.end()) {
        memoryUsed_ -= size->second;
        resultSizes_.erase(size);
    }
    it->second.erase(it2);
    if (it->second.empty()) {
        activeRequests_.erase(it);
    }
    return true;
}

void ReadLimiter::finishRequest(uint32_t clientID, uint32_t requestID) noexcept {
    try {
        RemoteStore* dropped = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex_);

            if (!releaseActive(clientID, requestID)) {
                // Not sent yet (abandoned while queued), or already released (e.g. failed to be sent): drop it if
                // queued
                for (auto it = requests_.begin(); it != requests_.end(); ++it) {
                    if (it->client->id() == clientID && it->id == requestID) {
                        dropped = it->client;
                        requests_.erase(it);
                        break;
                    }
                }
            }
        }

        // A request that was never sent gets no Complete from the server: unregister it from its store
        if (dropped) {
            dropped->failRequest(requestID, std::make_exception_ptr(eckit::SeriousBug("Read request abandoned")));
        }
    }
    catch (...) {
        eckit::Log::error() << "ReadLimiter: failed to release read request " << requestID << std::endl;
    }

    tryNextRequests();
}

/// @note: Only called when a RemoteStore is destroyed, which is currently on exit.
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
            for (auto requestID : it->second) {
                limiter.memoryUsed_ -= limiter.resultSizes_[{clientID, requestID}];
                limiter.resultSizes_.erase({clientID, requestID});
            }
            limiter.activeRequests_.erase(it);
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
    // mutex_ released; tryNextRequests() will re-acquire it (and release before sendRequest)
    limiter.tryNextRequests();
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
