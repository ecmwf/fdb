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
#include "fdb5/remote/client/ClientConnectionRouter.h"

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

Client::Client(const eckit::net::Endpoint& endpoint) :
    endpoint_(endpoint) {}

uint32_t Client::controlWriteCheckResponse(Message msg, const void* payload, uint32_t payloadLength) {
    return ClientConnectionRouter::instance().controlWriteCheckResponse(*this, msg, payload, payloadLength);
}
uint32_t Client::controlWrite(Message msg, const void* payload, uint32_t payloadLength) {
    return ClientConnectionRouter::instance().controlWrite(*this, msg, payload, payloadLength);
}

uint32_t Client::dataWrite(Message msg, const void* payload, uint32_t payloadLength) {
    return ClientConnectionRouter::instance().dataWrite(*this, msg, payload, payloadLength);
}
void Client::dataWrite(uint32_t requestId, const void* data, size_t length){
    ClientConnectionRouter::instance().dataWrite(*this, requestId, data, length);
}

}