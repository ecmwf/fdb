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

constexpr uint16_t CurrentVersion = 4;


enum class Message : uint16_t {

    // Server instructions
    None = 0,
    Exit,

    // API calls to forward
    Flush,
    Archive,
    Retrieve,
    List,
    Dump,
    Where,
    Wipe,
    Purge,
    Stats,

    // Responses
    Received,
    Blob,
    MultiBlob,
    Complete,
    Error,
    ExpectedSize,
    Startup
};


// Header used for all messages

struct MessageHeader {

public: // methods

    MessageHeader() {}

    MessageHeader(Message message, uint32_t requestID, uint32_t payloadSize=0) :
        marker(StartMarker),
        version(CurrentVersion),
        message(message),
        requestID(requestID),
        payloadSize(payloadSize) {}

    eckit::FixedString<4> marker;   // 4 bytes  --> 4

    uint16_t version;               // 2 bytes  --> 6

    Message message;                // 2 bytes  --> 8

    uint32_t requestID;             // 4 bytes  --> 12

    uint32_t payloadSize;           // 4 bytes  --> 16

    eckit::FixedString<16> hash;    // 16 bytes --> 32
};


//----------------------------------------------------------------------------------------------------------------------

} // namespace remote
} // namespace fdb5

#endif // fdb5_remote_Messages_H
