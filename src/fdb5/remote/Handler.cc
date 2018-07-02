/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <unistd.h>
#include <algorithm>

#include "eckit/config/Resource.h"
#include "eckit/log/Log.h"
#include "eckit/log/Bytes.h"
#include "eckit/io/Buffer.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/io/DataHandle.h"
#include "eckit/maths/Functions.h"
#include "eckit/runtime/Main.h"

#include "fdb5/remote/Handler.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/database/Key.h"
#include "fdb5/LibFdb.h"

#include "marslib/MarsRequest.h"

using namespace eckit;

namespace fdb5 {
namespace remote {

//----------------------------------------------------------------------------------------------------------------------

RemoteHandler::RemoteHandler(eckit::TCPSocket& socket, const Config& config) :
    socket_(socket),
    fdb_(config),
    archiveQueue_(eckit::Resource<size_t>("fdbServerMaxQueueSize", 32)) {}


RemoteHandler::~RemoteHandler() {

    // If we have launched a thread with an async and we manage to get here, this is
    // an error. n.b. if we don't do something, we will block in the destructor
    // of std::future.

    if (archiveFuture_.valid()) {
        Log::error() << "Attempting to destruct RemoteHandler with active archive thread" << std::endl;
        eckit::Main::instance().terminate();
    }
}


void RemoteHandler::handle() {

    Log::info() << "... started" << std::endl;

    MessageHeader hdr;
    eckit::FixedString<4> tail;

    while (socket_.read(&hdr, sizeof(hdr))) {

        ASSERT(hdr.marker == StartMarker);
        ASSERT(hdr.version == CurrentVersion);

        switch (hdr.message) {

        case Message::Exit:
            Log::status() << "Exiting" << std::endl;
            return;

        case Message::Flush:
            flush();
            break;

        case Message::Archive:
            archive(hdr);
            break;

        case Message::Retrieve:
            retrieve(hdr);
            break;

        default: {
            std::stringstream ss;
            ss << "ERROR: Unexpected message recieved ("
               << static_cast<int>(hdr.message)
               << "). ABORTING";
            Log::status() << ss.str() << std::endl;
            Log::error() << "Retrieving... " << ss.str() << std::endl;
            throw SeriousBug(ss.str(), Here());
        }
        };

        ASSERT(socket_.read(&tail, 4));
        ASSERT(tail == EndMarker);
    }

    // If we have got here with the archive thread still running,
    // it is due to an unclean disconnection from the client between writing fields.
    // Try as hard as possible!

    if (archiveFuture_.valid()) {
        flush();
        throw eckit::ReadError("Client connection unexpectedly lost. Archived fields may be incomplete", Here());
    }
}

void RemoteHandler::flush() {
    Log::status() << "Queueing data flush" << std::endl;
    //Log::debug<LibFdb>() << "Flushing data" << std::endl;
    Log::info() << "Queueing data flush" << std::endl;

    // If we have a worker thread running (i.e. data has been written), then signal
    // a flush to it, and wait for it to be done.

    if (archiveFuture_.valid()) {

        try {
            archiveQueue_.emplace(std::pair<Key, Buffer>(Key{}, 0));
            archiveFuture_.get();
        }
        catch(const eckit::Exception& e) {
            std::string what(e.what());
            MessageHeader response(Message::Error, what.length());
            socket_.write(&response, sizeof(response));
            socket_.write(what.c_str(), what.length());
            socket_.write(&EndMarker, sizeof(EndMarker));
            throw;
        }
    }

    Log::status() << "Flush complete" << std::endl;
    //Log::debug<LibFdb>() << "Flushing data" << std::endl;
    Log::info() << "Flush complete" << std::endl;

    MessageHeader response(Message::Complete);
    socket_.write(&response, sizeof(response));
    socket_.write(&EndMarker, sizeof(EndMarker));
}

void RemoteHandler::archive(const MessageHeader& hdr) {

    // Ensure that we have a handling thread running

    if (!archiveFuture_.valid()) {
        archiveFuture_ = std::async(std::launch::async, [this] { archiveThreadLoop(); });
    }

    // Ensure we have somewhere to read the data from the socket

    if (!archiveBuffer_ || archiveBuffer_->size() < hdr.payloadSize) {
        archiveBuffer_.reset(new Buffer( eckit::round(hdr.payloadSize, 4*1024) ));
    }

    // Extract the data

    ASSERT(hdr.payloadSize > 0);
    socket_.read(*archiveBuffer_, hdr.payloadSize);

    MemoryStream keyStream(*archiveBuffer_);
    fdb5::Key key(keyStream);

    std::stringstream ss_key;
    ss_key << key;
    Log::status() << "Queueing: " << ss_key.str() << std::endl;
    Log::debug<LibFdb>() << "Queueing: " << ss_key.str() << std::endl;

    size_t pos = keyStream.position();
    size_t len = hdr.payloadSize - pos;

    // Queue the data for asynchronous handling

    Buffer buffer(&(*archiveBuffer_)[pos], len);

    size_t queuelen = archiveQueue_.emplace(std::make_pair(std::move(key), Buffer(&(*archiveBuffer_)[pos], len)));
    Log::status() << "Queued (" << queuelen << "): " << ss_key.str() << std::endl;
    Log::debug<LibFdb>() << "Queued (" << queuelen << "): " << ss_key.str() << std::endl;
}

void RemoteHandler::retrieve(const MessageHeader& hdr) {

    Log::status() << "Retrieving..." << std::endl;
    Log::debug<LibFdb>() << "Retrieving... " << std::endl;

    size_t requiredBuffer = std::max(int(hdr.payloadSize), 64 * 1024 * 1024);

    if (!retrieveBuffer_ || retrieveBuffer_->size() < requiredBuffer) {
        retrieveBuffer_.reset(new Buffer(requiredBuffer + 4096 - (((requiredBuffer - 1) % 4096) + 1)));
    }

    ASSERT(hdr.payloadSize > 0);
    socket_.read(*retrieveBuffer_, hdr.payloadSize);

    MemoryStream requestStream(*retrieveBuffer_);
    MarsRequest request(requestStream);

    // Read the data into a buffer, and then write it to the stream.
    // For now, we assume that we are dealing with one field, and that as such it will fit into
    // the buffer...

    size_t bytesRead;

    try {
        eckit::ScopedPtr<DataHandle> dh(fdb_.retrieve(request));

        dh->openForRead();
        bytesRead = dh->read(*retrieveBuffer_, requiredBuffer);
    }
    catch(const eckit::Exception& e) {
        std::string what(e.what());
        MessageHeader response(Message::Error, what.length());
        socket_.write(&response, sizeof(response));
        socket_.write(what.c_str(), what.length());
        socket_.write(&EndMarker, sizeof(EndMarker));
        throw;
    }

    // ASSERT(bytesRead > 0); // May be NO fields found. That is legit.
    ASSERT(bytesRead < requiredBuffer);

    MessageHeader response(Message::Blob, bytesRead);

    socket_.write(&response, sizeof(response));
    socket_.write(*retrieveBuffer_, bytesRead);
    socket_.write(&EndMarker, sizeof(EndMarker));

    Log::status() << "Done retrieve" << std::endl;
    Log::debug<LibFdb>() << "Done retrieve" << std::endl;
}


void RemoteHandler::archiveThreadLoop() {

    std::pair<Key, Buffer> element {Key{}, 0};

    while (true) {

        size_t queuelen = archiveQueue_.pop(element);

        const Key& key(element.first);
        const Buffer& buffer(element.second);

        if (buffer.size() == 0) {
            ASSERT(key.empty());

            Log::info() << "Flushing" << std::endl;
            Log::status() << "Flushing" << std::endl;
            fdb_.flush();
            Log::status() << "Flush complete" << std::endl;
            return;
        }

        std::stringstream ss_key;
        ss_key << key;
        Log::info() << "Archived (" << queuelen << "): " << ss_key.str() << std::endl;
        Log::status() << "Archived (" << queuelen << "): " << ss_key.str() << std::endl;
        fdb_.archive(key, buffer, buffer.size());
        Log::status() << "Archive done (" << queuelen << "): " << ss_key.str() << std::endl;
    }
}


//----------------------------------------------------------------------------------------------------------------------

} // namespace remote
} // namespace fdb5

