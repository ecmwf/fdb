// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "fdb5/remote/Messages.h"
#include "fdb5/tracing/TraceContext.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/net/TCPSocket.h"
#include "eckit/os/BackTrace.h"
#include "eckit/serialisation/MemoryStream.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <mutex>
#include <string_view>
#include <vector>

namespace eckit {

class Buffer;
class Value;

}  // namespace eckit

namespace fdb5 {

class Config;

}
namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

class TCPException : public eckit::Exception {
public:

    TCPException(const std::string& msg, const eckit::CodeLocation& here) :
        eckit::Exception(std::string("TCPException: ") + msg, here) {}
};

//----------------------------------------------------------------------------------------------------------------------

class Connection {

public:  // types

    using PayloadList = std::vector<Payload>;

public:  // methods

    Connection();

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;
    Connection(Connection&&) = delete;
    Connection& operator=(Connection&&) = delete;

    virtual ~Connection() = default;

    void write(Message msg, bool control, uint32_t clientID, uint32_t requestID, PayloadList payloads = {}) const;

    void write(Message msg, bool control, uint32_t clientID, uint32_t requestID, const void* data,
               uint32_t length) const {
        write(msg, control, clientID, requestID, {{length, data}});
    }

    /// As write(), preceded by a Message::Trace frame carrying @p trace if trace-context propagation was negotiated
    /// for this connection and @p trace is valid. Both frames are written under the same socket lock, so the Trace
    /// frame always immediately precedes the frame it describes.
    void write(Message msg, bool control, uint32_t clientID, uint32_t requestID, const PayloadList& payloads,
               const tracing::TraceContext& trace) const;

    /// Negotiated trace-context propagation version, 0 if not agreed
    int tracingVersion() const { return tracingVersion_; }

    void error(bool control, std::string_view msg, uint32_t clientID, uint32_t requestID) const;
    void unauthorised(std::string_view msg, uint32_t clientID, uint32_t requestID) const;

    eckit::Buffer readControl(MessageHeader& hdr) const;

    eckit::Buffer readData(MessageHeader& hdr) const;

    void teardown();

    bool valid() const { return isValid_; }

private:  // methods

    eckit::Buffer read(bool control, MessageHeader& hdr) const;

    void writeUnsafe(const eckit::net::TCPSocket& socket, const void* data, size_t length) const;

    /// Write one frame (header, payloads, end marker). The caller must hold the socket mutex.
    void writeFrameUnsafe(const eckit::net::TCPSocket& socket, Message msg, bool control, uint32_t clientID,
                          uint32_t requestID, const PayloadList& payloads) const;

    bool readUnsafe(const eckit::net::TCPSocket& socket, void* data, size_t length) const;

    const eckit::net::TCPSocket& getSocket(bool control) const;

    std::mutex& getSocketMutex(bool control) const;

    virtual const eckit::net::TCPSocket& controlSocket() const = 0;

    virtual const eckit::net::TCPSocket& dataSocket() const = 0;

protected:  // members

    bool single_;

    /// Set from the agreed RemoteConfiguration at startup; 0 means no Message::Trace frames on this connection
    int tracingVersion_{0};

private:  // members

    std::atomic<bool> closingSocket_{false};

    mutable std::mutex controlMutex_;
    mutable std::mutex dataMutex_;

    /// Indicates if this instance is in a usable state.
    /// Once this is marked as invalid it cannot be recovered.
    mutable std::atomic<bool> isValid_{true};
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
