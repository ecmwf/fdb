#include "eckit/io/Buffer.h"
#include "eckit/log/Log.h"

#include <cstdint>
#include <mutex>
#include <string_view>
#include "fdb5/LibFdb5.h"
#include "fdb5/remote/Connection.h"
#include "fdb5/remote/Messages.h"

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

Connection::Connection() : single_(false) {}

void Connection::teardown() {
    closingSocket_ = true;

    if (!valid()) {
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
        ASSERT(hdr.marker == MessageHeader::StartMarker);
        ASSERT(hdr.version == MessageHeader::currentVersion);
        ASSERT(single_ || hdr.control() == control);

        eckit::Buffer payload{hdr.payloadSize};
        if ((hdr.payloadSize == 0 || readUnsafe(socket, payload, hdr.payloadSize))
            // Ensure we have consumed exactly the correct amount from the socket.
            && readUnsafe(socket, &tail, sizeof(tail))) {

            ASSERT(tail == MessageHeader::EndMarker);
            return payload;
        }
    }

    hdr.message = Message::Exit;
    return eckit::Buffer{0};
}

void Connection::write(const Message msg, const bool control, const uint32_t clientID, const uint32_t requestID,
                       const PayloadList payloads) const {

    uint32_t payloadLength = 0;
    for (const auto& payload : payloads) {
        ASSERT(payload.data);
        payloadLength += payload.length;
    }

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

void Connection::error(std::string_view msg, uint32_t clientID, uint32_t requestID) const {
    eckit::Log::error() << "[clientID=" << clientID << ",requestID=" << requestID << "]  " << msg << std::endl;
    write(Message::Error, false, clientID, requestID, msg.data(), msg.length());
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
