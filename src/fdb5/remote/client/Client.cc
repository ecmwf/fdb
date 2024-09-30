/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "fdb5/LibFdb5.h"

#include "fdb5/remote/client/Client.h"
#include "fdb5/remote/client/ClientConnectionRouter.h"
// #include <random>

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

void Client::setClientID() {
    static std::mutex idMutex_;
    // static std::random_device rd;
    // static std::mt19937 mt(rd());
    // static std::uniform_int_distribution<int> dist(0, 1000);
    static uint32_t clientId_ = 0; // dist(mt)*1000;

    std::lock_guard<std::mutex> lock(idMutex_);
    id_ = ++clientId_;
}

Client::Client(const eckit::net::Endpoint& endpoint, const std::string& defaultEndpoint) :
    connection_(ClientConnectionRouter::instance().connection(endpoint, defaultEndpoint)) {

    setClientID();
    connection_.add(*this);
}

Client::Client(const std::vector<std::pair<eckit::net::Endpoint, std::string>>& endpoints) :
    connection_(ClientConnectionRouter::instance().connection(endpoints)) {

    setClientID();
    connection_.add(*this);
}

Client::~Client() {
    connection_.remove(id_);
}

void Client::controlWriteCheckResponse(Message msg, uint32_t requestID, bool dataListener, const void* payload, uint32_t payloadLength) {

    ASSERT(requestID);
    ASSERT(!(!payloadLength ^ !payload));
    std::lock_guard<std::mutex> lock(blockingRequestMutex_);

    std::vector<std::pair<const void*, uint32_t>> data;
    if (payloadLength) {
        data.push_back(std::make_pair(payload, payloadLength));
    }

    std::future<eckit::Buffer> f = connection_.controlWrite(*this, msg, requestID, dataListener, data);
    f.wait();
    ASSERT(f.get().size() == 0);
}

eckit::Buffer Client::controlWriteReadResponse(Message msg, uint32_t requestID, const void* payload, uint32_t payloadLength) {

    ASSERT(requestID);
    ASSERT(!(!payloadLength ^ !payload));
    std::lock_guard<std::mutex> lock(blockingRequestMutex_);
    
    std::vector<std::pair<const void*, uint32_t>> data{};
    if (payloadLength) {
        data.push_back(std::make_pair(payload, payloadLength));
    }

    std::future<eckit::Buffer> f = connection_.controlWrite(*this, msg, requestID, false, data);
    f.wait();
    return eckit::Buffer{f.get()};
}

void Client::dataWrite(remote::Message msg, uint32_t requestID, std::vector<std::pair<const void*, uint32_t>> data) {
    connection_.dataWrite(*this, msg, requestID, data);
}

// void Client::handleException(std::exception_ptr e) {
//     try
//     {
//         if (e)
//             std::rethrow_exception(e);
//     }
//     catch(const std::exception& e)
//     {
//         eckit::Log::error() << "error: " << e.what();
//         ClientConnectionRouter::instance().teardown();
//     }
// }

} // namespace fdb5::remote