/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/remote/client/ReadLimiter.h"
#include <mutex>
#include <sstream>
#include "eckit/config/Resource.h"
#include "fdb5/remote/client/RemoteStore.h"

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
    memoryLimit_ = memoryLimit;
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
    if (requests_.empty()) {
        return false;
    }

    const auto& request = requests_.front();

    if (memoryUsed_ + request.resultSize > memoryLimit_) {
        return false;
    }

    activeRequests_[request.client->id()].insert(request.id);
    resultSizes_[{request.client->id(), request.id}] = request.resultSize;
    memoryUsed_ += request.resultSize;

    sendRequest(request);

    requests_.pop_front();
    return true;
}

void ReadLimiter::finishRequest(uint32_t clientID, uint32_t requestID) {
    {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = activeRequests_.find(clientID);
        if (it == activeRequests_.end()) {
            return;
        }

        auto it2 = it->second.find(requestID);
        ASSERT(it2 != it->second.end());

        memoryUsed_ -= resultSizes_[{clientID, requestID}];
        it->second.erase(it2);
        resultSizes_.erase({clientID, requestID});
    }

    tryNextRequest();
}

/// @note: Only called when a RemoteStore is destroyed, which is currently on exit.
void ReadLimiter::evictClient(size_t clientID) {
    std::lock_guard<std::mutex> lock(instanceMutex_);
    if (instance_ != nullptr) {
        std::lock_guard<std::mutex> lock(instance_->mutex_);
        // Remove the client's active requests
        auto it = instance_->activeRequests_.find(clientID);

        if (it != instance_->activeRequests_.end()) {
            for (auto requestID : it->second) {
                instance_->memoryUsed_ -= instance_->resultSizes_[{clientID, requestID}];
                instance_->resultSizes_.erase({clientID, requestID});
            }
            instance_->activeRequests_.erase(it);
        }

        // Clean up any pending requests attributed to this client
        ///@note O(n), room for optimisation.
        auto it2 = instance_->requests_.begin();
        while (it2 != instance_->requests_.end()) {
            if (it2->client->id() == clientID) {
                it2 = instance_->requests_.erase(it2);
            }
            else {
                ++it2;  // Only increment if we didn't erase
            }
        }
        instance_->tryNextRequest();

        if (instance_->activeRequests_.empty() && instance_->requests_.empty()) {
            // If there are no more active or pending requests, we can reset the instance to free memory.
            instance_.reset();
        }
    }
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
