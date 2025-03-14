/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#pragma once

#include "eckit/io/Buffer.h"
#include "eckit/memory/NonCopyable.h"
#include "eckit/serialisation/MemoryStream.h"

#include "fdb5/database/Key.h"
#include "fdb5/remote/RemoteFieldLocation.h"

#include <cstdint>
#include <deque>
#include <map>
#include <mutex>

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

struct RequestInfo {
    const RemoteStore* client;  // Non-owning
    uint32_t id;                // note request ids are only unique per connection. Client id is unique within a process
    eckit::Buffer requestBuffer;  // Payload to send to the server
    size_t requestSize;
    size_t resultSize;  // Size of the field obtained from the store
};

// Holds a queue of all of the currently unfulfilled read requests.
// Prevents asking the servers for more data until we have consumed the data we have already received.
/// @note: Does not own any result buffers, just keeps track of their expected sizes.
/// @todo: In future, we will have more fine-grained memory limits on individual queues.
class ReadLimiter : eckit::NonCopyable {
public:

    static ReadLimiter& instance();

    static void init(size_t memoryLimit);

    // Add a new request to the queue of requests to be sent. Will not be sent until we know we have buffer space.
    void add(RemoteStore* client, uint32_t id, const FieldLocation& fieldLocation,
             const Key& remapKey);  // use const *?

    // Attempt to send the next request in the queue. Returns true if a request was sent.
    // If not enough memory is available, or there is no next request, returns false.
    bool tryNextRequest();

    void finishRequest(uint32_t clientID, uint32_t requestID);

    // When a RemoteStore is destroyed, it must evict any unconsumed requests.
    // If all went well, there will be no requests to evict, but we must check (consumer may have abandoned the
    // request).
    /// @todo: This is somewhat pointless right now because the RemoteStores appear to be infinitely long lived...
    /// Revisit if this changes.
    void evictClient(size_t clientID);

    // Debugging
    void print(std::ostream& out) const;

private:

    ReadLimiter(size_t memoryLimit);

    // Send the request to the server
    void sendRequest(const RequestInfo& request) const;

private:

    mutable std::mutex mutex_;

    size_t memoryUsed_;
    const size_t memoryLimit_;

    // Enqueued requests
    std::deque<RequestInfo> requests_;

    // client id -> request id
    std::map<uint32_t, std::set<uint32_t>> activeRequests_;

    // {clientID, requestID} -> result size in bytes
    std::map<std::pair<uint32_t, uint32_t>, size_t> resultSizes_;
};

}  // namespace fdb5::remote
