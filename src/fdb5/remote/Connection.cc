#include "fdb5/remote/Connection.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/remote/Messages.h"

#include "eckit/io/Buffer.h"
#include "eckit/log/Log.h"

#include <sys/socket.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <limits>
#include <mutex>
#include <sstream>
#include <string_view>
#include <thread>
#include <vector>

namespace fdb5::remote {

namespace {
// How long an error may wait for room in the send buffer of the peer
constexpr int errorSendTimeoutMicroseconds = 200 * 1000;
}  // namespace

//----------------------------------------------------------------------------------------------------------------------

Connection::Connection() : single_(false) {}

void Connection::teardown() {
    const bool wasValid = isValid_.exchange(false);
    if (closingSocket_.exchange(true)) {
        return;
    }

    if (!wasValid) {
        return;
    }

    if (!single_) {
        // TODO make the data connection dying automatically, when there are no more async writes
        try {
            // all done - disconnecting
            Connection::write(Message::Exit, false, 0, 0);
        }
        catch (...) {
            // if connection is already down, no need to escalate
        }
    }
    try {
        // all done - disconnecting
        Connection::write(Message::Exit, true, 0, 0);
    }
    catch (...) {
        // if connection is already down, no need to escalate
    }
}

void Connection::abortConnection() noexcept {
    isValid_ = false;
    closingSocket_ = true;  // blocked reads must return quietly rather than raise a read error

    if (aborted_.exchange(true)) {
        return;
    }

    shutdownSocket(true);
    if (!single_) {
        shutdownSocket(false);
    }
}

void Connection::closeSockets() noexcept {
    abortConnection();

    const auto close = [this](bool control) {
        try {
            std::lock_guard<std::mutex> lock(getSocketMutex(control));
            const_cast<eckit::net::TCPSocket&>(getSocket(control)).close();
        }
        catch (...) {
            // not established, or already closed
        }
    };
    close(true);
    if (!single_) {
        close(false);
    }
}

void Connection::shutdownSocket(bool control) noexcept {
    try {
        // Shutting down does not modify the socket object, it only wakes up whoever is blocked on it.
        auto& socket = const_cast<eckit::net::TCPSocket&>(getSocket(control));
        try {
            socket.closeInput();
        }
        catch (...) {
        }
        try {
            socket.closeOutput();
        }
        catch (...) {
        }
    }
    catch (...) {
        // socket not established yet or already closed
    }
}

//----------------------------------------------------------------------------------------------------------------------

void Connection::writeUnsafe(const eckit::net::TCPSocket& socket, const void* const data, const size_t length) const {
    long written = socket.write(data, length);
    if (written < 0) {
        isValid_ = false;
        std::ostringstream ss;
        ss << "Write error. Expected " << length << ". Error = " << eckit::Log::syserr;
        throw TCPException(ss.str(), Here());
    }
    if (length != written) {
        isValid_ = false;
        std::ostringstream ss;
        ss << "Write error. Expected " << length << " bytes, wrote " << written;
        throw TCPException(ss.str(), Here());
    }
}

bool Connection::readUnsafe(const eckit::net::TCPSocket& socket, void* data, size_t length) const {
    long read = socket.read(data, length);
    if (length != read) {
        isValid_ = false;
        if (closingSocket_) {
            return false;
        }
        std::ostringstream ss;
        ss << "Read error. Expected " << length << " bytes";
        if (read > 0) {
            ss << ", read " << read << " bytes";
        }
        ss << ". Error = " << eckit::Log::syserr;
        throw TCPException(ss.str(), Here());
    }
    return true;
}

eckit::Buffer Connection::read(bool control, MessageHeader& hdr) const {
    const auto& socket = getSocket(control);
    eckit::FixedString<4> tail;
    hdr.payloadSize = 0;
    if (readUnsafe(socket, &hdr, sizeof(hdr))) {
        // Nothing read from the peer is trusted: a failed check fails the connection, it must not crash the process
        if (!(hdr.marker == MessageHeader::StartMarker)) {
            isValid_ = false;
            throw RemoteProtocolException("invalid start of message, the stream is out of sync", Here());
        }
        if (hdr.version != MessageHeader::currentVersion) {
            isValid_ = false;
            std::ostringstream ss;
            ss << "unsupported message version " << hdr.version << ", expected " << MessageHeader::currentVersion
               << " (client and server versions are not compatible)";
            throw RemoteProtocolException(ss.str(), Here());
        }
        if (!(single_ || hdr.control() == control)) {
            isValid_ = false;
            std::ostringstream ss;
            ss << (control ? "data" : "control") << " message " << hdr.message << " received on the "
               << (control ? "control" : "data") << " connection";
            throw RemoteProtocolException(ss.str(), Here());
        }
        if (hdr.payloadSize > maxPayloadSize_) {
            isValid_ = false;
            std::ostringstream ss;
            ss << "message " << hdr.message << " with a payload of " << hdr.payloadSize
               << " bytes exceeds the limit of " << maxPayloadSize_ << " bytes";
            throw RemoteProtocolException(ss.str(), Here());
        }

        eckit::Buffer payload{hdr.payloadSize};
        if ((hdr.payloadSize == 0 || readUnsafe(socket, payload, hdr.payloadSize))
            // Ensure we have consumed exactly the correct amount from the socket.
            && readUnsafe(socket, &tail, sizeof(tail))) {

            if (!(tail == MessageHeader::EndMarker)) {
                isValid_ = false;
                throw RemoteProtocolException("invalid end of message, the stream is out of sync", Here());
            }
            return payload;
        }
    }

    // Peer disconnected: report it as an Exit that does not carry the identifiers of the previous message
    hdr.message = Message::Exit;
    hdr.clientID_ = control ? 1 : 0;
    hdr.requestID = 0;
    hdr.payloadSize = 0;
    return eckit::Buffer{0};
}

void Connection::write(const Message msg, const bool control, const uint32_t clientID, const uint32_t requestID,
                       const PayloadList payloads) const {

    size_t totalLength = 0;
    for (const auto& payload : payloads) {
        ASSERT(payload.data);
        totalLength += payload.length;
    }
    // the header carries the length on 32 bits: a larger payload would desynchronise the stream
    if (totalLength > std::numeric_limits<uint32_t>::max()) {
        std::ostringstream ss;
        ss << "Cannot send message " << msg << ": its payload of " << totalLength
           << " bytes exceeds the limit of the protocol (4 GiB)";
        throw eckit::UserError(ss.str(), Here());
    }
    const auto payloadLength = static_cast<uint32_t>(totalLength);

    MessageHeader message{msg, control, clientID, requestID, payloadLength};

    const auto& socket = getSocket(control);
    std::lock_guard<std::mutex> lock(getSocketMutex(control));

    LOG_DEBUG_LIB(LibFdb5) << "Connection::write [endpoint=" << socket.remoteHost() << ":" << socket.remotePort()
                           << ",message=" << msg << ",clientID=" << message.clientID() << ",control=" << control
                           << ",requestID=" << requestID << ",payloadsSize=" << payloads.size()
                           << ",payloadLength=" << payloadLength << "]" << std::endl;


    writeUnsafe(socket, &message, sizeof(message));

    for (const auto& payload : payloads) {
        writeUnsafe(socket, payload.data, payload.length);
    }

    writeUnsafe(socket, &MessageHeader::EndMarker, MessageHeader::markerBytes);
}

void Connection::error(bool control, std::string_view msg, uint32_t clientID, uint32_t requestID) const {
    eckit::Log::error() << "[clientID=" << clientID << ",requestID=" << requestID << "]  " << msg << std::endl;
    write(Message::Error, control, clientID, requestID, msg.data(), msg.length());
}

bool Connection::tryError(bool control, std::string_view msg, uint32_t clientID, uint32_t requestID) const noexcept {
    try {
        error(control, msg, clientID, requestID);
        return true;
    }
    catch (...) {
        // connection already down, nobody left to tell
        return false;
    }
}

bool Connection::tryWriteBounded(Message message, bool control, uint32_t clientID, uint32_t requestID,
                                 std::string_view payload) const noexcept {
    try {
        // Neither the mutex nor the socket may block for long: the peer may have stopped reading, with a full send
        // buffer, and the caller has to abort the connection to release the workers.
        // Another writer usually holds the mutex only briefly: wait for it, within the same bound as the send, rather
        // than dropping the error of a client that is reading normally.
        std::unique_lock<std::mutex> lock(getSocketMutex(control), std::defer_lock);
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::microseconds(errorSendTimeoutMicroseconds);
        while (!lock.try_lock()) {
            if (std::chrono::steady_clock::now() >= deadline) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        constexpr size_t maxMessageSize = 4096;
        const auto length = static_cast<uint32_t>(std::min(payload.length(), maxMessageSize));

        const MessageHeader header{message, control, clientID, requestID, length};
        std::vector<char> frame(sizeof(header) + length + MessageHeader::markerBytes);
        std::memcpy(frame.data(), &header, sizeof(header));
        if (length > 0) {
            std::memcpy(frame.data() + sizeof(header), payload.data(), length);
        }
        std::memcpy(frame.data() + sizeof(header) + length, &MessageHeader::EndMarker, MessageHeader::markerBytes);

        // Qualified: TCPServer overrides socket() to return the listening socket, not the accepted one
        auto& tcpSocket = const_cast<eckit::net::TCPSocket&>(getSocket(control));
        const int fd = tcpSocket.eckit::net::TCPSocket::socket();

        // The send timeout only affects the writes, not the threads that are reading from the same socket. The
        // mutex is held, so nobody else writes meanwhile.
        timeval previous{};
        socklen_t previousLength = sizeof(previous);
        if (::getsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &previous, &previousLength) != 0) {
            return false;
        }
        const timeval bound{0, errorSendTimeoutMicroseconds};
        if (::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &bound, sizeof(bound)) != 0) {
            return false;
        }

        int flags = 0;
#ifdef MSG_NOSIGNAL
        flags |= MSG_NOSIGNAL;
#endif
        // If the timeout expires with only part of the frame sent, the peer is left with a truncated frame: the
        // caller aborts the connection right after, so the peer sees a lost connection.
        const auto sent = ::send(fd, frame.data(), frame.size(), flags);

        ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &previous, previousLength);

        return sent == static_cast<ssize_t>(frame.size());
    }
    catch (...) {
        return false;
    }
}

bool Connection::tryErrorBounded(bool control, std::string_view msg, uint32_t clientID,
                                 uint32_t requestID) const noexcept {
    try {
        eckit::Log::error() << "[clientID=" << clientID << ",requestID=" << requestID << "]  " << msg << std::endl;
    }
    catch (...) {
    }
    return tryWriteBounded(Message::Error, control, clientID, requestID, msg);
}

void Connection::endConnection() noexcept {
    // n.b. like teardown(), the peer is told only if the connection was still valid, and only once
    const bool wasValid = isValid_.exchange(false);
    if (wasValid && !closingSocket_.exchange(true)) {
        // Best effort, bounded: another writer may hold the socket (e.g. blocked on a full send buffer, the peer
        // having stopped reading), and only the abort below releases it. An Exit that cannot be sent is skipped: the
        // peer sees a lost connection instead.
        if (!single_) {
            tryWriteBounded(Message::Exit, false, 0, 0, {});
        }
        tryWriteBounded(Message::Exit, true, 0, 0, {});
    }
    abortConnection();
}

void Connection::unauthorised(std::string_view msg, uint32_t clientID, uint32_t requestID) const {
    eckit::Log::warning() << "[clientID=" << clientID << ",requestID=" << requestID << "]  " << msg << std::endl;
    write(Message::Unauthorised, true, clientID, requestID, msg.data(), msg.length());
}


eckit::Buffer Connection::readControl(MessageHeader& hdr) const {
    return read(true, hdr);
}

eckit::Buffer Connection::readData(MessageHeader& hdr) const {
    return read(false, hdr);
}

const eckit::net::TCPSocket& Connection::getSocket(bool control) const {
    if (control || single_) {
        return controlSocket();
    }
    return dataSocket();
}

std::mutex& Connection::getSocketMutex(bool control) const {
    if (control || single_) {
        return controlMutex_;
    }
    return dataMutex_;
}


}  // namespace fdb5::remote
