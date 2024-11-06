#include "eckit/io/Buffer.h"
#include "eckit/log/Log.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/remote/Connection.h"

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

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

void Connection::writeUnsafe(bool control, const void* data, size_t length) {
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

void Connection::readUnsafe(bool control, void* data, size_t length) {
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

eckit::Buffer Connection::read(bool control, MessageHeader& hdr) {
    eckit::FixedString<4> tail;

    std::lock_guard<std::mutex> lock((control || single_) ? readControlMutex_ : readDataMutex_);
    readUnsafe(control, &hdr, sizeof(hdr));

    // std::cout << "READ [" <<  "endpoint=" <<  ((control || single_) ? controlSocket() : dataSocket()).remotePort() << ",message=" << hdr.message << ",clientID=" << hdr.clientID() << ",requestID=" << hdr.requestID << ",payload=" << hdr.payloadSize << "]" << std::endl;

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
        // std::cout << "ERROR while reading: ";

        char msg[hdr.payloadSize+1];
        if (hdr.payloadSize) {
            char msg[hdr.payloadSize+1];
            // std::cout << static_cast<char*>(payload.data());
        }
        // std::cout << std::endl;
    }

    return payload;
}

// void Connection::write(remote::Message msg, bool control, const Handler& clientID, uint32_t requestID, const void* data, uint32_t length) {
//     write(msg, control, clientID.clientId(), requestID, std::vector<std::pair<const void*, uint32_t>>{{data, length}});
// }
// void Connection::write(remote::Message msg, bool control, const Handler& clientID, uint32_t requestID, std::vector<std::pair<const void*, uint32_t>> data) {
//     write(msg, control, clientID.clientId(), requestID, data);
// }
void Connection::write(remote::Message msg, bool control, uint32_t clientID, uint32_t requestID, const void* data, uint32_t length) {
    write(msg, control, clientID, requestID, std::vector<std::pair<const void*, uint32_t>>{{data, length}});
}
void Connection::write(remote::Message msg, bool control, uint32_t clientID, uint32_t requestID, std::vector<std::pair<const void*, uint32_t>> data) {

    uint32_t payloadLength = 0;
    for (auto d: data) {
        ASSERT(d.first);
        payloadLength += d.second;
    }

    MessageHeader message{msg, control, clientID, requestID, payloadLength};

    // std::cout << "WRITE [" << "endpoint=" << ((control || single_) ? controlSocket() : dataSocket()).remotePort() << ",message=" << message.message << ",clientID=" << message.clientID() << ",requestID=" << message.requestID << ",payload=" << message.payloadSize << "]" << std::endl;

    LOG_DEBUG_LIB(LibFdb5) << "Connection::write [message=" << msg << ",clientID=" << message.clientID() << ",control=" << control << ",requestID=" << requestID << ",data=" << data.size() << ",payload=" << payloadLength << "]" << std::endl;

    std::lock_guard<std::mutex> lock((control || single_) ? controlMutex_ : dataMutex_);
    writeUnsafe(control, &message, sizeof(message));
    for (auto d: data) {
        writeUnsafe(control, d.first, d.second);
    }
    writeUnsafe(control, &EndMarker, sizeof(EndMarker));
}

// void Connection::writeControl(remote::Message msg, uint32_t clientID, uint32_t requestID, const void* data, uint32_t length) {
//     write(msg, true, clientID, requestID, std::vector<std::pair<const void*, uint32_t>>{{data, length}});
// }
// void Connection::writeControl(remote::Message msg, uint32_t clientID, uint32_t requestID, std::vector<std::pair<const void*, uint32_t>> data) {
//     write(msg, true, clientID, requestID, data);
// }
// void Connection::writeData(remote::Message msg, uint32_t clientID, uint32_t requestID, const void* data, uint32_t length) {
//     write(msg, false, clientID, requestID, std::vector<std::pair<const void*, uint32_t>>{{data, length}});
// }
// void Connection::writeData(remote::Message msg, uint32_t clientID, uint32_t requestID, std::vector<std::pair<const void*, uint32_t>> data) {
//     write(msg, false, clientID, requestID, data);
// }
void Connection::error(const std::string& msg, uint32_t clientID, uint32_t requestID) {
    eckit::Log::error() << "[clientID=" << clientID << ",requestID=" << requestID << "]  " << msg << std::endl;
    write(Message::Error, false, clientID, requestID, std::vector<std::pair<const void*, uint32_t>>{{msg.c_str(), msg.length()}});
}
// void Connection::error(const std::string& msg, const Handler& clientID, uint32_t requestID) {
//     write(Message::Error, true, clientID.clientId(), requestID, std::vector<std::pair<const void*, uint32_t>>{{msg.c_str(), msg.length()}});
// }
eckit::Buffer Connection::readControl(MessageHeader& hdr) {
    return read(true, hdr);
}
eckit::Buffer Connection::readData(MessageHeader& hdr) {
    return read(false, hdr);
}

}  // namespace fdb5::remote
