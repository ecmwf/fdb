/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <functional>

#include "fdb5/LibFdb.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/api/RemoteFDB.h"

#include "eckit/config/LocalConfiguration.h"
#include "eckit/io/Buffer.h"
#include "eckit/log/Bytes.h"
#include "eckit/log/Log.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/utils/Translator.h"

#include "marslib/MarsRequest.h"

// TODO: Write things as one chunk?

using namespace eckit;


namespace fdb5 {
namespace remote {

static FDBBuilder<RemoteFDB> remoteFdbBuilder("remote");

//----------------------------------------------------------------------------------------------------------------------

class TCPException : public Exception {
public:
    TCPException(const std::string& msg, const CodeLocation& here) :
        Exception(std::string("TCPException: ") + msg, here) {}
};

//----------------------------------------------------------------------------------------------------------------------

RemoteFDB::RemoteFDB(const eckit::Configuration& config) :
    FDBBase(config),
    hostname_(config.getString("host")),
    port_(config.getLong("port")),
    connected_(false) {

    ASSERT(config.getString("type", "") == "remote");
}

RemoteFDB::~RemoteFDB() {
    disconnect();
}


void RemoteFDB::connect() {
    if (!connected_) {
        client_.connect(hostname_, port_);
//        stream_.reset(new TCPStream(client_.connect(hostname_, port_)));
        connected_ = true;
    }
}


void RemoteFDB::disconnect() {
    if (connected_) {
        MessageHeader msg(Message::Exit);
        clientWrite(&msg, sizeof(msg));
        clientWrite(&EndMarker, sizeof(EndMarker));
        client_.close();
//        msg.encode(*stream_);
//        stream_.reset();
        connected_ = false;
    }
}

void RemoteFDB::clientWrite(const void *data, size_t length) {
    size_t written = client_.write(data, length);
    if (length != written) {
        std::stringstream ss;
        ss << "Write error. Expected " << length << " bytes, wrote " << written;
        throw TCPException(ss.str(), Here());
    }
}

void RemoteFDB::clientRead(void *data, size_t length) {
    size_t read = client_.read(data, length);
    if (length != read) {
        std::stringstream ss;
        ss << "Read error. Expected " << length << " bytes, read " << read;
        throw TCPException(ss.str(), Here());
    }
}

void RemoteFDB::handleError(const MessageHeader& hdr) {

    ASSERT(hdr.marker == StartMarker);
    ASSERT(hdr.version == CurrentVersion);

    if (hdr.message == Message::Error) {
        ASSERT(hdr.payloadSize > 9);

        std::string what(hdr.payloadSize, ' ');
        clientRead(&what[0], hdr.payloadSize);
        what[hdr.payloadSize] = 0; // Just in case

        try {
            eckit::FixedString<4> tail;
            clientRead(&tail, sizeof(tail));
        } catch (...) {}

        throw RemoteException(what, hostname_);
    }
}


void RemoteFDB::archive(const Key& key, const void* data, size_t length) {

    eckit::Log::info() << "Archiving..." << std::endl;

    connect();

    Buffer keyBuffer(4096);
    MemoryStream keyStream(keyBuffer);
    keyStream << key;

    MessageHeader msg(Message::Archive, length + keyStream.position());

    clientWrite(&msg, sizeof(msg));
    clientWrite(keyBuffer, keyStream.position());
    clientWrite(data, length);
    clientWrite(&EndMarker, sizeof(EndMarker));

    // And wait until done (this may be overkill...)

    MessageHeader response;
    eckit::FixedString<4> tail;

    clientRead(&response, sizeof(MessageHeader));

    handleError(response);

    ASSERT(response.marker == StartMarker);
    ASSERT(response.version == CurrentVersion);
    ASSERT(response.message == Message::Complete);

    clientRead(&tail, 4);
    ASSERT(tail == EndMarker);
}


/// A data handle that pulls the specified amount of data from a socket into a buffer,
/// and then stores it there until requested.

class RetrieveDataHandle : public DataHandle {

public: // methods

    RetrieveDataHandle(size_t length) :
        length_(length),
        pos_(0),
        buffer_(length),
        read_(false) {}

    /// This is only not in the constructor to avoid the risk of exceptions being thrown
    /// in the constructor. Not well supported until std::make_unique in c++14
    void doRetrieve(const std::function<void(void*, size_t)>& readfn) {
        readfn(buffer_, length_);
        read_ = true;
    }

    virtual void print(std::ostream& s) const { s << "RetrieveDataHandle(size=" << Bytes(length_) << ")";}

    virtual Length openForRead() { return length_; }
    virtual void close() {}

    virtual long read(void* pos, long sz) {

        ASSERT(read_);
        ASSERT(length_ >= pos_);

        if (sz >= (length_ - pos_)) {
            sz = (length_ - pos_);
        }

        if (sz > 0) {
            ::memcpy(pos, &buffer_[pos_], sz);
            pos_ += sz;
        }

        ASSERT(length_ >= pos_);
        return sz;
    }


    virtual void openForWrite(const Length&) { NOTIMP; }
    virtual void openForAppend(const Length&) { NOTIMP; }

    virtual long write(const void*,long) { NOTIMP; }

private: // methods

    size_t pos_;
    size_t length_;
    Buffer buffer_;
    bool read_;
};



eckit::DataHandle* RemoteFDB::retrieve(const MarsRequest& request) {

    eckit::Log::info() << "Retrieving ..." << std::endl;

    connect();

    Buffer requestBuffer(4096);
    MemoryStream requestStream(requestBuffer);
    request.encode(requestStream);

    MessageHeader msg(Message::Retrieve, requestStream.position());

    clientWrite(&msg, sizeof(msg));
    clientWrite(requestBuffer, requestStream.position());
    clientWrite(&EndMarker, sizeof(EndMarker));

    MessageHeader response;
    eckit::FixedString<4> tail;

    clientRead(&response, sizeof(MessageHeader));

    handleError(response);

    ASSERT(response.marker == StartMarker);
    ASSERT(response.version == CurrentVersion);
    ASSERT(response.message == Message::Blob);

    eckit::ScopedPtr<RetrieveDataHandle> dh(new RetrieveDataHandle(response.payloadSize));
    dh->doRetrieve([this](void* data, size_t len) { this->clientRead(data, len); });

    clientRead(&tail, 4);
    ASSERT(tail == EndMarker);

    eckit::Log::info() << "Retrieved " << response.payloadSize << " bytes!!!!" << std::endl;

    return dh.release();
}


std::string RemoteFDB::id() const {
    eckit::Translator<size_t, std::string> toStr;
    return std::string("remote=") + hostname_ + ":" + toStr(port_);
}


void RemoteFDB::flush() {
    MessageHeader msg(Message::Flush);
    clientWrite(&msg, sizeof(msg));
    clientWrite(&EndMarker, sizeof(EndMarker));

    MessageHeader response;
    eckit::FixedString<4> tail;

    clientRead(&response, sizeof(MessageHeader));

    handleError(response);

    ASSERT(response.marker == StartMarker);
    ASSERT(response.version == CurrentVersion);
    ASSERT(response.message == Message::Complete);

    clientRead(&tail, 4);
    ASSERT(tail == EndMarker);
}


void RemoteFDB::print(std::ostream &s) const {
    s << "RemoteFDB(host=" << "port=" << ")";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace remote
} // namespace fdb5
