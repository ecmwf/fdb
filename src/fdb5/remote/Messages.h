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

#include "eckit/io/Buffer.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/types/FixedString.h"

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iosfwd>

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

struct BufferStream;

struct Payload {
    Payload() = default;

    explicit Payload(const BufferStream& buffer);

    Payload(std::size_t length, const void* data);

    bool empty() const;

    /// @brief  Checks if this object is in a consistent state.
    /// @returns True if (length & data) is (zero & null) or (non-zero & non-null).
    bool consistent() const;

    std::size_t length {0};
    const void* data {nullptr};
};

struct BufferStream : private eckit::Buffer, public eckit::MemoryStream {
    explicit BufferStream(const size_t size) : eckit::Buffer(size), eckit::MemoryStream(data(), size) { }

    size_t length() const { return eckit::MemoryStream::position(); }

    const void* data() const { return eckit::Buffer::data(); }

    Payload payload() const { return {length(), data()}; }
};

//----------------------------------------------------------------------------------------------------------------------

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
    Overlay,

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
    static constexpr uint16_t currentVersion = 12;

    static constexpr uint16_t hashBytes = 16;

    static constexpr uint16_t markerBytes = 4;

    using MarkerType = eckit::FixedString<markerBytes>;

    using HashType = eckit::FixedString<hashBytes>;

    inline static const MarkerType StartMarker {"SFDB"};

    inline static const MarkerType EndMarker {"EFDB"};

public:  // methods
    MessageHeader() = default;

    MessageHeader(Message message, bool control, uint32_t clientID, uint32_t requestID, uint32_t payloadSize);

    bool control() const {
        return ((clientID_ & 0x00000001) == 1);
    }
    uint32_t clientID() const {
        return (clientID_>>1);
    }

public:
    MarkerType marker;                    // 4 bytes  --> 4
    uint16_t   version {currentVersion};  // 2 bytes  --> 6
    Message    message {Message::None};   // 2 bytes  --> 8
    uint32_t   clientID_ {0};             // 4 bytes  --> 12
    uint32_t   requestID {0};             // 4 bytes  --> 16
    uint32_t   payloadSize {0};           // 4 bytes  --> 20
    HashType   hash;                      // 16 bytes --> 36
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5::remote
