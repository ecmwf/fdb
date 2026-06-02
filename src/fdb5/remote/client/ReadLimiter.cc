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

void ReadLimiter::init(size_t memoryLimit) {
    std::lock_guard<std::mutex> lock(instanceMutex_);
    if (instance_ == nullptr) {
        instance_.reset(new ReadLimiter(memoryLimit));
    }
}

ReadLimiter& ReadLimiter::instance() {
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
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        requests_.emplace_back(RequestInfo{client, id, std::move(requestBuffer), requestSize, resultSize});
    }

    tryNextRequest();
}

bool ReadLimiter::tryNextRequest() {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
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
        std::lock_guard<std::recursive_mutex> lock(mutex_);

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
        auto& instance = *instance_;
        std::lock_guard<std::recursive_mutex> lock(instance.mutex_);
        // Remove the client's active requests
        auto it = instance.activeRequests_.find(clientID);

        if (it != instance.activeRequests_.end()) {
            for (auto requestID : it->second) {
                instance.memoryUsed_ -= instance.resultSizes_[{clientID, requestID}];
                instance.resultSizes_.erase({clientID, requestID});
            }
            instance.activeRequests_.erase(it);
        }

        // Clean up any pending requests attributed to this client
        ///@note O(n), room for optimisation.
        auto it2 = instance.requests_.begin();
        while (it2 != instance.requests_.end()) {
            if (it2->client->id() == clientID) {
                it2 = instance.requests_.erase(it2);
            }
            else {
                ++it2;  // Only increment if we didn't erase
            }
        }
        instance.tryNextRequest();
    }
}

void ReadLimiter::print(std::ostream& out) const {
    std::lock_guard<std::recursive_mutex> lock(mutex_);

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
