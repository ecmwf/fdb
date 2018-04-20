/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @date   Apr 2018

#ifndef fdb5_remote_Messages_H
#define fdb5_remote_Messages_H

#include "eckit/types/FixedString.h"
#include "eckit/serialisation/Streamable.h"

#include <cstdint>

namespace eckit {
    class Stream;
}


namespace fdb5 {
namespace remote {

//----------------------------------------------------------------------------------------------------------------------

const static eckit::FixedString<4> StartMarker {"SFDB"};
const static eckit::FixedString<4> EndMarker {"EFDB"};

constexpr uint16_t CurrentVersion = 1;


enum class Message : uint16_t {
    None = 0,
    Exit,
    Flush,
    Archive,
    Retrieve,
    Blob,
    Complete,
    Error
};


// Header used for all messages

struct MessageHeader {

public: // methods

    MessageHeader() {}

    MessageHeader(Message message, uint32_t payloadSize=0) :
        marker(StartMarker),
        version(CurrentVersion),
        message(message),
        payloadSize(payloadSize) {}

    eckit::FixedString<4> marker;

    uint16_t version;

    Message message;

    uint32_t payloadSize;
};


// // Header used for all messages
//
// class MessageHeader : public eckit::Streamable {
//
// public: // methods
//
//     MessageHeader(Message message, uint16_t payloadSize=0);
//     MessageHeader(eckit::Stream& s);
//
//     // From Streamable
//
//     virtual void encode(eckit::Stream& s) const;
//     virtual const eckit::ReanimatorBase& reanimator() const { return reanimator_; }
//
//     static  const eckit::ClassSpec&  classSpec()        { return classSpec_; }
//
// public: // members
//
//     eckit::FixedString<4> marker;
//
//     uint8_t version;
//
//     Message message;
//
//     uint16_t payloadSize;
//
// private:
//     static  eckit::ClassSpec               classSpec_;
//     static  eckit::Reanimator<MessageHeader>  reanimator_;
// };

//----------------------------------------------------------------------------------------------------------------------

} // namespace remote
} // namespace fdb5

#endif // fdb5_remote_Messages_H
