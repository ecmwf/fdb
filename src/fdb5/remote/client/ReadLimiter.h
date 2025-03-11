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

#include <cstdint>
#include <mutex>

#include "eckit/memory/NonCopyable.h"
#include "eckit/io/Buffer.h"
#include "eckit/serialisation/MemoryStream.h"

#include "fdb5/remote/RemoteFieldLocation.h"
#include "fdb5/database/Key.h"

namespace fdb5::remote {
    
//----------------------------------------------------------------------------------------------------------------------

struct RequestInfo {
    // uint32_t clientid;
    const RemoteStore* client; // Feels dangerous... || Maybe we can just hold a reference to the client and let them do the work?
    uint32_t id; // note request ids are only unique per connection. Client id is unique within a process
    eckit::Buffer requestBuffer; // Payload to send to the server
    size_t requestSize;
    size_t resultSize; // Expected size of the field obtained from the store
};

// Holds a queue of all of the current read requests.
// It also knows which requests have already been buffered (e.g. owned by the RemoteStore) and in particular, how large they were.
// We do not ask the servers for more data until we have consumed the data we have already received.
// The consumer is responsible for letting us know when it has consumed the data.
// Note this singleton does not own any buffers, it just keeps track of the requests and their sizes.
class ReadLimiter : eckit::NonCopyable {
public:

    static ReadLimiter& instance();

    // Add a new request to the queue of requests to be sent. Will not be sent until we know we have buffer space.
    // Importantly, request has a size in bytes associated with it (worked out from field location .length())
    void add(RemoteStore* client, uint32_t id, const FieldLocation& fieldLocation, const Key& remapKey); // use const *?

    // Attempt to send the next request in the queue. Returns true if a request was sent.
    // If not enough memory is available, or there is no next request, returns false.
    bool tryNextRequest();

    void finishRequest(uint32_t clientID, uint32_t requestID);

    // If a client dies, it must evict all of its requests from the limiter.
    // This should normally do nothing, as the client should have already finished all of its requests.
    // But must be called in case of exceptions, or e.g. if the consumer decides to stop consuming .
    void evictClient(size_t clientID);

    void print(std::ostream& out) const;

private:

    ReadLimiter();

    void _sendRequest(const RequestInfo& request) const;

private: 
    using RequestID = uint32_t;
    mutable std::mutex mutex_;

    size_t memoryUsed_;
    const size_t memoryLimit_;

    // Waiting requests
    std::deque<RequestInfo> requests_;

    // <reqid -> size of result> : Currently active requests
    std::map<RequestID, size_t> bufferedRequests_; // We maybe don't need to actually hold on to this, if the client remembers the size. But maybe client does not remember.
    // clientid -> reqid
    std::map<uint32_t, std::set<RequestID>> clientRequests_;

};

}  // namespace fdb5::remote
