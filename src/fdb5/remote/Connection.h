// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "fdb5/remote/Messages.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/net/TCPSocket.h"
#include "eckit/os/BackTrace.h"
#include "eckit/serialisation/MemoryStream.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
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

/// The peer sent something that does not follow the protocol (bad framing, unsupported version, oversized or
/// inconsistent message). Input from the network is never trusted with an ASSERT.
class RemoteProtocolException : public eckit::Exception {
public:

    RemoteProtocolException(const std::string& msg, const eckit::CodeLocation& here) :
        eckit::Exception(std::string("Remote FDB protocol error: ") + msg, here) {}
};

/// A message was not received within its deadline
class ConnectionTimeout : public TCPException {
public:

    ConnectionTimeout(const std::string& msg, const eckit::CodeLocation& here) : TCPException(msg, here) {}
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

    /// Same as tryError(), but bounded: it waits at most 200 ms for another thread writing on the socket, and at most
    /// 200 ms for room in the send buffer of the peer (the message is truncated to 4 KiB). To be used where nothing
    /// may block on a socket writer, e.g. right before aborting the connection. Returns false if the error could not
    /// be sent completely.
    bool tryErrorBounded(bool control, std::string_view msg, uint32_t clientID, uint32_t requestID) const noexcept;

    eckit::Buffer readControl(MessageHeader& hdr) const;

    eckit::Buffer readData(MessageHeader& hdr) const;

    /// Graceful disconnection: notifies the peer with an Exit message.
    void teardown();

    /// Disconnection after a failure: notifies the peer with an Exit message if the connection was still valid, but
    /// without waiting for long (see tryErrorBounded()), then aborts the connection (see abortConnection()). Unlike
    /// teardown(), it never blocks on another writer of the socket, whom the abort releases. Never throws.
    void endConnection() noexcept;

    /// Abrupt disconnection after a failure: shuts down both sockets so that every thread blocked on them is
    /// released (reads then behave as if the peer had disconnected, writes fail). No Exit message is sent, so that
    /// the peer sees a lost connection rather than a graceful end. Idempotent, thread safe, never throws.
    void abortConnection() noexcept;

    /// True once abortConnection() has been called
    bool aborted() const { return aborted_; }

    bool valid() const { return isValid_; }

private:  // methods

    // Validation of what is read from the peer (see read())
    void checkHeader(const MessageHeader& hdr, bool control) const;
    void checkTail(const MessageHeader::MarkerType& tail) const;

    // Reads exactly length bytes from the control socket before the deadline
    void readBefore(void* data, size_t length, std::chrono::steady_clock::time_point deadline) const;

    // Writes a whole frame, waiting at most 200 ms for the socket mutex and 200 ms for room in the send buffer
    bool tryWriteBounded(Message message, bool control, uint32_t clientID, uint32_t requestID,
                         std::string_view payload) const noexcept;

    eckit::Buffer read(bool control, MessageHeader& hdr) const;

    void writeUnsafe(const eckit::net::TCPSocket& socket, const void* data, size_t length) const;

    bool readUnsafe(const eckit::net::TCPSocket& socket, void* data, size_t length) const;

    const eckit::net::TCPSocket& getSocket(bool control) const;

    std::mutex& getSocketMutex(bool control) const;

    virtual const eckit::net::TCPSocket& controlSocket() const = 0;

    virtual const eckit::net::TCPSocket& dataSocket() const = 0;

protected:  // methods

    /// Largest payload accepted from the peer: a larger one fails the read, before anything is allocated
    void maxPayloadSize(uint32_t size) { maxPayloadSize_ = size; }

    /// The connection cannot be used any more (e.g. the peer ended it)
    void invalidate() { isValid_ = false; }

    /// Reads a whole control message (header, payload and end marker) before the deadline, e.g. the answer to the
    /// startup message: unlike readControl(), a peer that stops (or slows down) in the middle of a message cannot
    /// block the caller beyond the deadline. Only while nothing else reads the control socket.
    /// @throws ConnectionTimeout if the message is not complete by then
    eckit::Buffer readControlBefore(MessageHeader& hdr, std::chrono::steady_clock::time_point deadline) const;

    /// Releases the file descriptors of a connection that is over (failed or ended), once nothing reads the sockets
    /// any more. They are shut down first, which wakes up a writer blocked on a full send buffer; then each one is
    /// closed under its mutex: a writer holds it for a whole frame, so a socket is never closed under a write, and a
    /// later write fails (bad descriptor) rather than writing to a descriptor reused meanwhile. Never throws.
    void closeSockets() noexcept;

    /// Shuts down one socket (both directions), waking up whoever is blocked on it, without marking the connection as
    /// aborted. Never throws.
    void shutdownSocket(bool control) noexcept;

protected:  // members

    bool single_;

private:  // members

    std::atomic<bool> closingSocket_{false};
    std::atomic<bool> aborted_{false};
    std::atomic<uint32_t> maxPayloadSize_{std::numeric_limits<uint32_t>::max()};

    mutable std::mutex controlMutex_;
    mutable std::mutex dataMutex_;

    /// Indicates if this instance is in a usable state.
    /// Once this is marked as invalid it cannot be recovered.
    mutable std::atomic<bool> isValid_{true};
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
