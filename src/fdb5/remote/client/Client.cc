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

#include "fdb5/remote/client/Client.h"
#include "fdb5/remote/client/ClientConnectionRouter.h"

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

Client::Client(const eckit::net::Endpoint& endpoint) :
    endpoint_(endpoint),
    connection_(*(ClientConnectionRouter::instance().connection(*this))),
    blockingRequestId_(0) {}

// uint32_t Client::generateRequestID() {
//     return connection_.generateRequestID();
// }


bool Client::response(uint32_t requestID) {
    ASSERT(requestID == blockingRequestId_);

    eckit::Buffer buf;
    payload_.set_value(std::move(buf));
    return true;
}

bool Client::response(uint32_t requestID, eckit::Buffer&& payload) {
    ASSERT(requestID == blockingRequestId_);

    payload_.set_value(std::move(payload));
    return true;
}

uint32_t Client::controlWriteCheckResponse(Message msg, const void* payload, uint32_t payloadLength) {
    uint32_t id = connection_.generateRequestID();
    controlWriteCheckResponse(msg, id, payload, payloadLength);
    return id;
}

void Client::controlWriteCheckResponse(Message msg, uint32_t requestID, const void* payload, uint32_t payloadLength) {
    ASSERT(!blockingRequestId_);
    ASSERT(requestID);

    payload_ = {};
    blockingRequestId_=requestID;
    std::future<eckit::Buffer> f = payload_.get_future();

    if (payloadLength) {
        connection_.controlWrite(*this, msg, blockingRequestId_, std::vector<std::pair<const void*, uint32_t>>{{payload, payloadLength}});
    } else {
        connection_.controlWrite(*this, msg, blockingRequestId_);
    }

    eckit::Buffer buf = f.get();
    blockingRequestId_=0;
    ASSERT(buf.size() == 0);
}
eckit::Buffer Client::controlWriteReadResponse(Message msg, const void* payload, uint32_t payloadLength) {
    ASSERT(!blockingRequestId_);

    payload_ = {};
    blockingRequestId_=connection_.generateRequestID();
    std::future<eckit::Buffer> f = payload_.get_future();

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
    connection_.dataWrite(msg, requestID, data);
}

// uint32_t Client::controlWrite(Message msg, const void* payload, uint32_t payloadLength) {
//     uint32_t id = connection_.generateRequestID();
//     connection_.controlWrite(*this, msg, id, payload, payloadLength);
//     return id;
// }

// uint32_t Client::dataWrite(Message msg, const void* payload, uint32_t payloadLength) {
//     uint32_t id = connection_.generateRequestID();
//     return connection_.dataWrite(msg, id, payload, payloadLength);
//     return id;
// }
// void Client::dataWrite(const void* data, size_t length){
//     connection_.dataWrite(data, length);
// }

}