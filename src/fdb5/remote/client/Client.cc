/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/remote/client/Client.h"

#include "fdb5/remote/Connection.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/remote/client/ClientConnectionRouter.h"

#include "eckit/config/Configuration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/io/Buffer.h"
#include "eckit/log/Log.h"
#include "eckit/net/Endpoint.h"

#include <cstdint>
#include <future>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

void Client::setClientID() {
    static std::mutex idMutex_;
    static uint32_t clientId_ = 0;

    std::lock_guard<std::mutex> lock(idMutex_);
    id_ = ++clientId_;
}

Client::Client(const eckit::Configuration& config) :
    Client(config, eckit::net::Endpoint{config.getString("host"), config.getInt("port")}, "") {}

Client::Client(const eckit::Configuration& config, const eckit::net::Endpoint& endpoint,
               const std::string& defaultEndpoint) :
    connection_(ClientConnectionRouter::instance().connection(config, endpoint, defaultEndpoint)) {

    setClientID();
    connection_->add(*this);
}

Client::Client(const eckit::Configuration& config,
               const std::vector<std::pair<eckit::net::Endpoint, std::string>>& endpoints) :
    connection_(ClientConnectionRouter::instance().connection(config, endpoints)) {

    setClientID();
    connection_->add(*this);
}

void Client::refreshConnection() {
    if (connection_->valid()) {
        return;
    }
    eckit::Log::warning() << "Connection to " << connection_->controlEndpoint()
                          << " is invalid, attempting to reconnect" << std::endl;
    connection_->remove(id_);
    connection_ = ClientConnectionRouter::instance().refresh(clientConfig(), connection_);
    connection_->add(*this);
}

Client::~Client() {
    connection_->remove(id_);
}

void Client::controlWriteCheckResponse(const Message msg, const uint32_t requestID, const bool dataListener,
                                       const void* const payload, const uint32_t payloadLength) const {

    ASSERT(requestID);
    ASSERT(!(!payloadLength ^ !payload));
    std::lock_guard<std::mutex> lock(blockingRequestMutex_);

    PayloadList payloads;
    if (payloadLength > 0) {
        payloads.emplace_back(payloadLength, payload);
    }

    auto f = connection_->controlWrite(*this, msg, requestID, dataListener, payloads);
    f.wait();
    ASSERT(f.get().size() == 0);
}

eckit::Buffer Client::controlWriteReadResponse(const Message msg, const uint32_t requestID, const void* const payload,
                                               const uint32_t payloadLength) const {

    ASSERT(requestID);
    ASSERT(!(!payloadLength ^ !payload));
    std::lock_guard<std::mutex> lock(blockingRequestMutex_);

    PayloadList payloads;
    if (payloadLength > 0) {
        payloads.emplace_back(payloadLength, payload);
    }

    auto f = connection_->controlWrite(*this, msg, requestID, false, payloads);
    f.wait();
    return eckit::Buffer{f.get()};
}

void Client::dataWrite(Message msg, uint32_t requestID, PayloadList payloads) {
    connection_->dataWrite(*this, msg, requestID, std::move(payloads));
}

}  // namespace fdb5::remote
