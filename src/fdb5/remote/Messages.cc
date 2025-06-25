/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#include "fdb5/remote/Messages.h"

// #include "eckit/serialisation/Stream.h"

using namespace eckit;


namespace fdb5 {
namespace remote {

//----------------------------------------------------------------------------------------------------------------------

// ClassSpec MessageHeader::classSpec_ = {&MessageHeader::classSpec(), "MessageHeader",};
// Reanimator<MessageHeader> MessageHeader::reanimator_;
//
//
// MessageHeader::MessageHeader(Message message, uint16_t payloadSize=0) :
//     marker(StartMarker),
//     version(CurrentVersion),
//     message(message),
//     payloadSize(payloadSize) {}
//
// MessageHeader::MessageHeader(Stream &s) :
//     marker(s) {
//     s >> version;
//     uint16_t tmp;
//     s >> tmp;
//     message = static_cast<Message>(tmp);
//     s >> payloadSize;
// }
//
// void MessageHeader::encode(Stream &s) const {
//     s << marker;
//     s << version;
//     s << static_cast<uint16_t>(message);
//     s << payloadSize;
// }

//----------------------------------------------------------------------------------------------------------------------

}  // namespace remote
}  // namespace fdb5
