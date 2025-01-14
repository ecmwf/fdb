#include "eckit/io/Buffer.h"
#include "eckit/log/Log.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/remote/Connection.h"
#include "fdb5/remote/Messages.h"

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

Connection::Connection() : single_(false) { }

void Connection::teardown() {

    if (!single_) {
        // TODO make the data connection dying automatically, when there are no more async writes
        try {
            // all done - disconnecting
            Connection::write(Message::Exit, false, 0, 0);
        } catch(...) {
            // if connection is already down, no need to escalate
        }
    }
    try {
        // all done - disconnecting
        Connection::write(Message::Exit, true, 0, 0);
    } catch(...) {
        // if connection is already down, no need to escalate
    }
}

//----------------------------------------------------------------------------------------------------------------------

void Connection::writeUnsafe(const bool control, const void* data, const size_t length) const {
    long written = 0;
    if (control || single_) {
        written = controlSocket().write(data, length);
    } else {
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
    } else {
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

eckit::Buffer Connection::read(bool control, MessageHeader& hdr) const {
    eckit::FixedString<4> tail;

    std::lock_guard<std::mutex> lock((control || single_) ? readControlMutex_ : readDataMutex_);
    readUnsafe(control, &hdr, sizeof(hdr));

    ASSERT(hdr.marker == StartMarker);
    ASSERT(hdr.version == CurrentVersion);
    ASSERT(single_ || hdr.control() == control);

    eckit::Buffer payload{hdr.payloadSize};
    if (hdr.payloadSize > 0) {
        readUnsafe(control, payload, hdr.payloadSize);
    }
    // Ensure we have consumed exactly the correct amount from the socket.
    readUnsafe(control, &tail, sizeof(tail));
    ASSERT(tail == EndMarker);

    if (hdr.message == Message::Error) {

        char msg[hdr.payloadSize+1];
        if (hdr.payloadSize) {
            char msg[hdr.payloadSize+1];
        }
    }

    return payload;
}

void Connection::write(Message msg, const bool control, const uint32_t clientID, const uint32_t requestID, Payload data) const {

    uint32_t payloadLength = 0;
    for (auto d: data) {
        ASSERT(d.first);
        payloadLength += d.second;
    }

    MessageHeader message{msg, control, clientID, requestID, payloadLength};

    LOG_DEBUG_LIB(LibFdb5) << "Connection::write [message=" << msg << ",clientID=" << message.clientID() << ",control=" << control << ",requestID=" << requestID << ",data=" << data.size() << ",payload=" << payloadLength << "]" << std::endl;

    std::lock_guard<std::mutex> lock((control || single_) ? controlMutex_ : dataMutex_);
    writeUnsafe(control, &message, sizeof(message));
    for (auto d: data) {
        writeUnsafe(control, d.first, d.second);
    }
    writeUnsafe(control, &EndMarker, sizeof(EndMarker));
}

void Connection::write(Message        msg,
                       const bool     control,
                       const uint32_t clientID,
                       const uint32_t requestID,
                       const void*    data,
                       const uint32_t length) const {
    write(msg, control, clientID, requestID, {{data, length}});
}

void Connection::error(const std::string& msg, uint32_t clientID, uint32_t requestID) const {
    eckit::Log::error() << "[clientID=" << clientID << ",requestID=" << requestID << "]  " << msg << std::endl;
    write(Message::Error, false, clientID, requestID, {{msg.c_str(), msg.length()}});
}

eckit::Buffer Connection::readControl(MessageHeader& hdr) const {
    return read(true, hdr);
}

eckit::Buffer Connection::readData(MessageHeader& hdr) const {
    return read(false, hdr);
}

}  // namespace fdb5::remote
