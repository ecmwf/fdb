// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "eckit/io/Buffer.h"
#include "eckit/serialisation/MemoryStream.h"

#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Key.h"

#include <cstdint>
#include <deque>
#include <map>
#include <mutex>


namespace fdb5::remote {

class RemoteStore;

//----------------------------------------------------------------------------------------------------------------------

struct RequestInfo {
    RemoteStore* client;        // Non-owning
    uint32_t id;                // note request ids are only unique per connection. Client id is unique within a process
    eckit::Buffer requestBuffer;  // Payload to send to the server
    size_t requestSize;
    size_t resultSize;  // Size of the field obtained from the store
};

// Holds a queue of all of the currently unfulfilled read requests.
// Prevents asking the servers for more data until we have consumed the data we have already received.
/// @note: Does not own any result buffers, just keeps track of their expected sizes.
/// @todo: In future, we will have more fine-grained memory limits on individual queues.
class ReadLimiter {
public:

    static ReadLimiter& instance();
    void setMemoryLimit(size_t memoryLimit);

    ReadLimiter(const ReadLimiter&) = delete;
    ReadLimiter& operator=(const ReadLimiter&) = delete;
    ReadLimiter(ReadLimiter&&) = delete;
    ReadLimiter& operator=(ReadLimiter&&) = delete;

    // Add a new request to the queue of requests to be sent. Will not be sent until we know we have buffer space.
    /// @throws if the field is larger than the memory limit, i.e. it could never be sent. A failure to send the
    ///         request is not thrown: it is delivered to the request (see RemoteStore::failRequest()).
    void add(RemoteStore* client, uint32_t id, const FieldLocation& fieldLocation,
             const Key& remapKey);  // use const *?

    /// The request is complete, or abandoned (e.g. its DataHandle is destroyed early): releases its budget if it was
    /// sent, removes it if it is still queued, and sends the next requests that fit.
    /// Never throws: it is called from destructors, and the failure to send another request belongs to that request.
    void finishRequest(uint32_t clientID, uint32_t requestID) noexcept;

    // When a RemoteStore is destroyed, it must evict any unconsumed requests.
    // If all went well, there will be no requests to evict, but we must check (consumer may have abandoned the
    // request).
    /// @todo: This is somewhat pointless right now because the RemoteStores appear to be infinitely long lived...
    /// Revisit if this changes.
    static void evictClient(size_t clientID);

    // Debugging
    void print(std::ostream& out) const;

private:

    ReadLimiter(size_t memoryLimit);

    static size_t defaultReadLimit();

    // Sends the queued requests, in order, as long as they fit in the memory limit. Never throws: a request that
    // cannot be sent releases its budget and is failed, so that its reader gets the error, not the caller.
    void tryNextRequests() noexcept;

    // Send the request to the server
    void sendRequest(const RequestInfo& request) const;

    // Releases the budget of a sent request. Caller holds mutex_. @returns false if the request was not active
    bool releaseActive(uint32_t clientID, uint32_t requestID);

private:

    mutable std::mutex mutex_;

    size_t memoryUsed_;
    size_t memoryLimit_;

    // Enqueued requests
    std::deque<RequestInfo> requests_;

    // client id -> request id
    std::map<uint32_t, std::set<uint32_t>> activeRequests_;

    // {clientID, requestID} -> result size in bytes
    std::map<std::pair<uint32_t, uint32_t>, size_t> resultSizes_;
};

}  // namespace fdb5::remote
