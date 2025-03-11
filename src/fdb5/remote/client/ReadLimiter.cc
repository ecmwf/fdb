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
#include "fdb5/remote/client/RemoteStore.h"
#include "eckit/config/Resource.h"
#include <sstream>

 namespace fdb5::remote {
     
 //----------------------------------------------------------------------------------------------------------------------
 
ReadLimiter& ReadLimiter::instance() {
    static ReadLimiter limiter;
    return limiter;
}

void ReadLimiter::add(RemoteStore* client, uint32_t id, const FieldLocation& fieldLocation, const Key& remapKey) {
    
    eckit::Buffer requestBuffer(4096);
    eckit::MemoryStream s(requestBuffer);
    s << fieldLocation;
    s << remapKey;
    size_t requestSize = s.position();
    
    size_t resultSize = fieldLocation.length();
    if(resultSize > memoryLimit_) {
        std::stringstream ss;
        ss << "ReadLimiter: Requested field size " << resultSize << " exceeds memory limit " << memoryLimit_ << ". Field: " << fieldLocation.fullUri();
        throw eckit::SeriousBug(ss.str());
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    // requests_.push(RequestInfo{client, id, std::move(requestBuffer), requestSize, resultSize});
    requests_.emplace_back(RequestInfo{client, id, std::move(requestBuffer), requestSize, resultSize});
    clientRequests_[client->id()].insert(id);
}

bool ReadLimiter::tryNextRequest() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (requests_.empty()) {
        return false;
    }

    const auto& request = requests_.front();
    
    if (memoryUsed_ + request.resultSize > memoryLimit_) {
        return false;
    }

    bufferedRequests_[request.id] = request.resultSize;
    memoryUsed_ += request.resultSize;

    _sendRequest(request);

    requests_.pop_front();
    return true;
}

void ReadLimiter::finishRequest(uint32_t clientID, uint32_t requestID) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = bufferedRequests_.find(requestID);
    ASSERT(it != bufferedRequests_.end()); // Double eviction is a bug.

    memoryUsed_ -= it->second;
    bufferedRequests_.erase(it);

    auto it2 = clientRequests_.find(clientID);
    ASSERT(it2 != clientRequests_.end());
    
    it2->second.erase(requestID);
    if (it2->second.empty()) {
        clientRequests_.erase(it2);
    }

}

// If a client dies, it must evict all of its requests from the limiter.
// This should normally do nothing, as the client should have already finished all of its requests.
// But must be called in case of exceptions, or e.g. if the consumer decides to stop consuming .
void ReadLimiter::evictClient(size_t clientID) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = clientRequests_.find(clientID);
    if (it == clientRequests_.end()) {
        return;
    }

    // Client left incomplete work so we have to do some cleanup.

    // Find any "active" requests which have become stale.
    for (uint32_t requestID : it->second) {
        auto it2 = bufferedRequests_.find(requestID);
        ASSERT(it2 != bufferedRequests_.end());
        memoryUsed_ -= it2->second;
        bufferedRequests_.erase(it2);
    }

    clientRequests_.erase(it);

    // Remove remaining pending requests
    requests_.erase(std::remove_if(requests_.begin(), requests_.end(), [&](const RequestInfo& request) {
        return request.client->id() == clientID;
    }), requests_.end());
}

void ReadLimiter::print(std::ostream& out) const {
    std::lock_guard<std::mutex> lock(mutex_);

    out << "ReadLimiter(memoryUsed=" << memoryUsed_ << ", memoryLimit=" << memoryLimit_ << ") {" << std::endl;

    out << "  Queued Requests: ";
    for (const auto& request : requests_) {
        out << request.id << " ";
    }
    out << std::endl;

    out << "  Buffered Requests: ";
    for (const auto& [reqid, size] : bufferedRequests_) {
        out << reqid << " ";
    }
    out << "}" << std::endl;
}
 

ReadLimiter::ReadLimiter():
    memoryUsed_{0},
        memoryLimit_{eckit::Resource<size_t>("$FDB_REMOTE_READ_LIMIT", 4 * 1024*1024*1024)} // 4GB
    {}

void ReadLimiter::_sendRequest(const RequestInfo& request) const {
    // xxx And if this fails?
    request.client->controlWriteCheckResponse(Message::Read, request.id, true, request.requestBuffer, request.requestSize);
}

 
 
}  // namespace fdb5::remote
 