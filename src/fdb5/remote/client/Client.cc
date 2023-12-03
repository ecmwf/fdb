/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <future>

#include "fdb5/LibFdb5.h"

#include "fdb5/remote/client/Client.h"
#include "fdb5/remote/client/ClientConnectionRouter.h"

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

uint32_t Client::clientId_=0;

Client::Client(const eckit::net::Endpoint& endpoint) :
    endpoint_(endpoint),
    connection_(*(ClientConnectionRouter::instance().connection(*this))),
    blockingRequestId_(0) {

    std::lock_guard<std::mutex> lock(idMutex_);
    id_ = ++clientId_;
}

Client::~Client() {
    ClientConnectionRouter::instance().deregister(*this);
}

bool Client::response(uint32_t requestID) {
    ASSERT(requestID == blockingRequestId_);

    eckit::Log::debug<LibFdb5>() << " response to blocking request " << requestID << std::endl;

    promise_.set_value(true);
    return true;
}

bool Client::response(uint32_t requestID, eckit::Buffer&& payload) {
    ASSERT(requestID == blockingRequestId_);

    eckit::Log::debug<LibFdb5>() << " response to blocking request " << requestID << " - payload size: " << payload.size() << std::endl;

    payloadPromise_.set_value(std::move(payload));
    return true;
}

// uint32_t Client::controlWriteCheckResponse(Message msg, const void* payload, uint32_t payloadLength) {
//     uint32_t id = connection_.generateRequestID();
//     controlWriteCheckResponse(msg, id, payload, payloadLength);
//     return id;
// }

void Client::controlWriteCheckResponse(Message msg, uint32_t requestID, const void* payload, uint32_t payloadLength) {

    ASSERT(requestID);
    std::lock_guard<std::mutex> lock(blockingRequestMutex_);
    
    promise_ = {};
    std::future<bool> f = promise_.get_future();

    blockingRequestId_=requestID;

    if (payloadLength) {
        connection_.controlWrite(*this, msg, blockingRequestId_, std::vector<std::pair<const void*, uint32_t>>{{payload, payloadLength}});
    } else {
        connection_.controlWrite(*this, msg, blockingRequestId_);
    }

    f.get();
    blockingRequestId_=0;
}

eckit::Buffer Client::controlWriteReadResponse(Message msg, uint32_t requestID, const void* payload, uint32_t payloadLength) {

    std::lock_guard<std::mutex> lock(blockingRequestMutex_);

    payloadPromise_ = {};
    std::future<eckit::Buffer> f = payloadPromise_.get_future();

    blockingRequestId_=requestID;

    if (payloadLength) {
        connection_.controlWrite(*this, msg, blockingRequestId_, std::vector<std::pair<const void*, uint32_t>>{{payload, payloadLength}});
    } else {
        connection_.controlWrite(*this, msg, blockingRequestId_);
    }

    eckit::Buffer buf = f.get();
    blockingRequestId_=0;

    return buf;
}

void Client::dataWrite(remote::Message msg, uint32_t requestID, std::vector<std::pair<const void*, uint32_t>> data) {
    connection_.dataWrite(*this, msg, requestID, data);
}

} // namespace fdb5::remote