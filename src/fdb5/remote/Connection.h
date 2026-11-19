// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "fdb5/remote/Messages.h"

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

    void error(bool control, std::string_view msg, uint32_t clientID, uint32_t requestID) const;
    void unauthorised(std::string_view msg, uint32_t clientID, uint32_t requestID) const;

    /// Same as error(), but never throws: to be used where an escaping exception would terminate the process
    /// (worker threads, destructors, cleanup paths). Returns false if the client could not be notified.
    bool tryError(bool control, std::string_view msg, uint32_t clientID, uint32_t requestID) const noexcept;

    /// Same as tryError(), but gives up instead of waiting if another thread is writing on the socket: to be used
    /// where nothing may block on a socket writer, e.g. right before aborting the connection.
    bool tryErrorNoWait(bool control, std::string_view msg, uint32_t clientID, uint32_t requestID) const noexcept;

    eckit::Buffer readControl(MessageHeader& hdr) const;

    eckit::Buffer readData(MessageHeader& hdr) const;

    /// Graceful disconnection: notifies the peer with an Exit message.
    void teardown();

    /// Abrupt disconnection after a failure: shuts down both sockets so that every thread blocked on them is
    /// released (reads then behave as if the peer had disconnected, writes fail). No Exit message is sent, so that
    /// the peer sees a lost connection rather than a graceful end. Idempotent, thread safe, never throws.
    void abortConnection() noexcept;

    /// True once abortConnection() has been called
    bool aborted() const { return aborted_; }

    bool valid() const { return isValid_; }

private:  // methods

    eckit::Buffer read(bool control, MessageHeader& hdr) const;

    /// The caller must hold the mutex of the socket
    void writeLocked(Message msg, bool control, uint32_t clientID, uint32_t requestID, PayloadList payloads,
                     uint32_t payloadLength) const;

    void writeUnsafe(const eckit::net::TCPSocket& socket, const void* data, size_t length) const;

    bool readUnsafe(const eckit::net::TCPSocket& socket, void* data, size_t length) const;

    const eckit::net::TCPSocket& getSocket(bool control) const;

    std::mutex& getSocketMutex(bool control) const;

    void shutdownSocket(bool control) noexcept;

    virtual const eckit::net::TCPSocket& controlSocket() const = 0;

    virtual const eckit::net::TCPSocket& dataSocket() const = 0;

protected:  // members

    bool single_;

private:  // members

    std::atomic<bool> closingSocket_{false};
    std::atomic<bool> aborted_{false};

    mutable std::mutex controlMutex_;
    mutable std::mutex dataMutex_;

    /// Indicates if this instance is in a usable state.
    /// Once this is marked as invalid it cannot be recovered.
    mutable std::atomic<bool> isValid_{true};
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
