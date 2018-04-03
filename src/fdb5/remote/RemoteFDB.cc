/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/config/LocalConfiguration.h"
#include "eckit/log/Log.h"
#include "eckit/utils/Translator.h"

#include "fdb5/LibFdb.h"
#include "fdb5/remote/RemoteFDB.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/io/HandleGatherer.h"

#include "eckit/io/Buffer.h"
#include "eckit/serialisation/MemoryStream.h"

// TODO: Write things as one chunk?

using namespace eckit;


namespace fdb5 {
namespace remote {

static FDBBuilder<RemoteFDB> remoteFdbBuilder("remote");

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
        socket_ = client_.connect(hostname_, port_);
//        stream_.reset(new TCPStream(client_.connect(hostname_, port_)));
        connected_ = true;
    }
}


void RemoteFDB::disconnect() {
    if (connected_) {
        MessageHeader msg(Message::Exit);
        socket_.write(&msg, sizeof(msg));
        socket_.write(&EndMarker, sizeof(EndMarker));
        socket_.close();
//        msg.encode(*stream_);
//        stream_.reset();
        connected_ = false;
    }
}


void RemoteFDB::archive(const Key& key, const void* data, size_t length) {

    eckit::Log::info() << "Archiving..." << std::endl;

    connect();

    Buffer keyBuffer(4096);
    MemoryStream keyStream(keyBuffer);
    keyStream << key;

    MessageHeader msg(Message::Archive, length + keyStream.position());

    Log::info() << "Sizes: "<< keyStream.position() << ", " << length << std::endl;

    socket_.write(&msg, sizeof(msg));
    socket_.write(keyBuffer, keyStream.position());
    socket_.write(data, length);
    socket_.write(&EndMarker, sizeof(EndMarker));
}


eckit::DataHandle *RemoteFDB::retrieve(const MarsRequest &request) {

    connect();
    NOTIMP;
}


std::string RemoteFDB::id() const {
    eckit::Translator<size_t, std::string> toStr;
    return std::string("remote=") + hostname_ + ":" + toStr(port_);
}


void RemoteFDB::flush() {
    MessageHeader msg(Message::Flush);
    socket_.write(&msg, sizeof(msg));
    socket_.write(&EndMarker, sizeof(EndMarker));
}


void RemoteFDB::print(std::ostream &s) const {
    s << "RemoteFDB(host=" << "port=" << ")";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace remote
} // namespace fdb5
