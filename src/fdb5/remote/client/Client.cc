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

Client::Client(eckit::net::Endpoint endpoint) :
    endpoints_(std::vector<eckit::net::Endpoint>{endpoint}) {}

Client::Client(std::vector<eckit::net::Endpoint>&& endpoints) :
    endpoints_(std::move(endpoints)) {}

uint32_t Client::controlWriteCheckResponse(Message msg, const void* payload, uint32_t payloadLength) {
    return ClientConnectionRouter::instance().controlWriteCheckResponse(endpoints_, *this, msg, payload, payloadLength);
}
uint32_t Client::controlWrite(Message msg, const void* payload, uint32_t payloadLength) {
    return ClientConnectionRouter::instance().controlWrite(endpoints_, *this, msg, payload, payloadLength);
}
// void Client::controlWrite(uint32_t requestId, const void* data, size_t length) {
//     ClientConnectionRouter::instance().controlWrite(*this, requestId, data, length);
// }
// void Client::controlRead(uint32_t requestId, void* data, size_t length) {
//     ClientConnectionRouter::instance().controlRead(*this, requestId, data, length);
// }

uint32_t Client::dataWrite(Message msg, const void* payload, uint32_t payloadLength) {
    return ClientConnectionRouter::instance().dataWrite(endpoints_, *this, msg, payload, payloadLength);
}
void Client::dataWrite(uint32_t requestId, const void* data, size_t length){
    ClientConnectionRouter::instance().dataWrite(*this, requestId, data, length);
}
// void Client::dataRead(uint32_t requestId, void* data, size_t length){
//     ClientConnectionRouter::instance().dataRead(*this, requestId, data, length);
// }

}