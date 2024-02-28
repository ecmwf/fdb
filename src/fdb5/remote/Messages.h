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

/// @author Simon Smart
/// @date   Apr 2018

#pragma once

#include <cstdint>
#include <cmath>

#include "eckit/types/FixedString.h"
// #include "fdb5/remote/Handler.h"

namespace eckit {
    class Stream;
}

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

const static eckit::FixedString<4> StartMarker {"SFDB"};
const static eckit::FixedString<4> EndMarker {"EFDB"};

constexpr uint16_t CurrentVersion = 12;

enum class Message : uint16_t {

    // Server instructions
    None = 0,
    Exit,
    Startup,
    Error,
    Stores,
    Schema,

    // API calls to forward
    Flush = 100,
    Archive,
    Retrieve,
    List,
    Dump,
    Status,
    Wipe,
    Purge,
    Stats,
    Control,
    Inspect,
    Read,
    Move,
    Store,

    // Responses
    Received = 200,
    Complete,

    // Data communication
    Blob = 300,
    MultiBlob

};

std::ostream& operator<<(std::ostream& s, const Message& m);

// Header used for all messages
class MessageHeader {

public: // methods

    MessageHeader() :
        version(CurrentVersion),
        message(Message::None),
        clientID_(0),
        requestID(0),
        payloadSize(0) {}

    // MessageHeader(Message message, bool control, const Handler& clientID, uint32_t requestID, uint32_t payloadSize=0);

    MessageHeader(Message message, bool control, uint32_t clientID, uint32_t requestID, uint32_t payloadSize);
    
    bool control() const {
        return ((clientID_ & 0x00000001) == 1);
    }
    uint32_t clientID() const {
        return (clientID_>>1);
    }

public:

    eckit::FixedString<4> marker;   // 4 bytes  --> 4

    uint16_t version;               // 2 bytes  --> 6

    Message message;                // 2 bytes  --> 8
 
    uint32_t clientID_;             // 4 bytes  --> 12

    uint32_t requestID;             // 4 bytes  --> 16

    uint32_t payloadSize;           // 4 bytes  --> 20

    eckit::FixedString<16> hash;    // 16 bytes --> 36

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5::remote
