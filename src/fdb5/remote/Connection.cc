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

    if (!single_) {
        // TODO make the data connection dying automatically, when there are no more async writes
        try {
            closingDataSocket_ = true;
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

void Connection::writeUnsafe(const bool control, const void* const data, const size_t length) const {
    long written = 0;
    if (control || single_) {
        written = controlSocket().write(data, length);
    }
    else {
        written = dataSocket().write(data, length);
    }
    if (written < 0) {
        std::stringstream ss;
        ss << "Write error. Expected " << length << ". Error = " << eckit::Log::syserr;
        throw TCPException(ss.str(), Here());
    }
    if (length != written) {
        std::stringstream ss;
        ss << "Write error. Expected " << length << " bytes, wrote " << written;
        throw TCPException(ss.str(), Here());
    }
}

void Connection::readUnsafe(bool control, void* data, size_t length) const {
    long read = 0;
    if (control || single_) {
        read = controlSocket().read(data, length);
    }
    else {
        read = dataSocket().read(data, length);
    }
    if (read < 0) {
        std::stringstream ss;
        ss << "Read error. Expected " << length << ". Error = " << eckit::Log::syserr;
        throw TCPException(ss.str(), Here());
    }
    if (length != read) {
        std::stringstream ss;
        ss << "Read error. Expected " << length << " bytes, read " << read;
        throw TCPException(ss.str(), Here());
    }
}

eckit::Buffer Connection::read(const bool control, MessageHeader& hdr) const {
    eckit::FixedString<4> tail;

    std::lock_guard<std::mutex> lock((control || single_) ? readControlMutex_ : readDataMutex_);
    readUnsafe(control, &hdr, sizeof(hdr));

    ASSERT(hdr.marker == MessageHeader::StartMarker);
    ASSERT(hdr.version == MessageHeader::currentVersion);
    ASSERT(single_ || hdr.control() == control);

    eckit::Buffer payload{hdr.payloadSize};
    if (hdr.payloadSize > 0) {
        readUnsafe(control, payload, hdr.payloadSize);
    }
    // Ensure we have consumed exactly the correct amount from the socket.
    readUnsafe(control, &tail, sizeof(tail));
    ASSERT(tail == MessageHeader::EndMarker);

    if (hdr.message == Message::Error) {

        char msg[hdr.payloadSize + 1];
        if (hdr.payloadSize) {
            char msg[hdr.payloadSize + 1];
        }
    }

    return payload;
}

void Connection::write(const Message msg, const bool control, const uint32_t clientID, const uint32_t requestID,
                       const PayloadList payloads) const {

    uint32_t payloadLength = 0;
    for (const auto& payload : payloads) {
        ASSERT(payload.data);
        payloadLength += payload.length;
    }

    MessageHeader message{msg, control, clientID, requestID, payloadLength};

    const auto& socket = control ? controlSocket() : dataSocket();
    LOG_DEBUG_LIB(LibFdb5) << "Connection::write [endpoint=" << socket.remoteHost() << ":" << socket.remotePort()
                           << ",message=" << msg << ",clientID=" << message.clientID() << ",control=" << control
                           << ",requestID=" << requestID << ",payloadsSize=" << payloads.size()
                           << ",payloadLength=" << payloadLength << "]" << std::endl;

    std::lock_guard<std::mutex> lock((control || single_) ? controlMutex_ : dataMutex_);

    writeUnsafe(control, &message, sizeof(message));

    for (const auto& payload : payloads) {
        writeUnsafe(control, payload.data, payload.length);
    }

    writeUnsafe(control, &MessageHeader::EndMarker, MessageHeader::markerBytes);
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

}  // namespace fdb5::remote
