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

#include <cmath>
#include <cstddef>
#include <cstdint>

#include "eckit/types/FixedString.h"

namespace eckit {
class Stream;
}

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

struct Payload {
    Payload(std::size_t length, const void* data) : length{length}, data{data} {}

    std::size_t length{0};
    const void* data{nullptr};
};

enum class Message : uint16_t {

    // Server instructions
    None = 0,
    Exit,
    Startup,
    Error,
    Stores,
    Schema,
    Stop,

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
    Axes,
    Exists,

    // Responses
    Received = 200,
    Complete,

    // Data communication
    Blob = 300,
    MultiBlob
};

std::ostream& operator<<(std::ostream& s, const Message& m);

//----------------------------------------------------------------------------------------------------------------------

// Header used for all messages
class MessageHeader {

public:  // types

    constexpr static uint16_t currentVersion = 12;

    constexpr static const auto hashBytes = 16;

    constexpr static const auto markerBytes = 4;

    using MarkerType = eckit::FixedString<markerBytes>;

    using HashType = eckit::FixedString<hashBytes>;

    inline static const MarkerType StartMarker{"SFDB"};

    inline static const MarkerType EndMarker{"EFDB"};

public:  // methods

    MessageHeader() = default;

    MessageHeader(Message message, bool control, uint32_t clientID, uint32_t requestID, uint32_t payloadSize);

    bool control() const { return ((clientID_ & 0x00000001) == 1); }
    uint32_t clientID() const { return (clientID_ >> 1); }

public:

    MarkerType marker;                 // 4 bytes  --> 4
    uint16_t version{currentVersion};  // 2 bytes  --> 6
    Message message{Message::None};    // 2 bytes  --> 8
    uint32_t clientID_{0};             // 4 bytes  --> 12
    uint32_t requestID{0};             // 4 bytes  --> 16
    uint32_t payloadSize{0};           // 4 bytes  --> 20
    HashType hash;                     // 16 bytes --> 36
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
