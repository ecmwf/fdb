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
    archiveWorker_(fdb_),
    useArchiveWorker_(eckit::Resource<bool>("fdbServerUseArchiveWorker", true)){}


RemoteHandler::~RemoteHandler() {}


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
            flush(hdr);
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
}

void RemoteHandler::flush(const MessageHeader&) {
    Log::status() << "Queueing data flush" << std::endl;
    //Log::debug<LibFdb>() << "Flushing data" << std::endl;
    Log::info() << "Queueing data flush" << std::endl;


    try {
        archiveWorker_.flush();
    }
    catch(const eckit::Exception& e) {
        std::string what(e.what());
        MessageHeader response(Message::Error, what.length());
        socket_.write(&response, sizeof(response));
        socket_.write(what.c_str(), what.length());
        socket_.write(&EndMarker, sizeof(EndMarker));
        throw;
    }

    Log::status() << "Flush complete" << std::endl;
    //Log::debug<LibFdb>() << "Flushing data" << std::endl;
    Log::info() << "Flush complete" << std::endl;

    MessageHeader response(Message::Complete);
    socket_.write(&response, sizeof(response));
    socket_.write(&EndMarker, sizeof(EndMarker));
}

void RemoteHandler::archive(const MessageHeader& hdr) {

    if (!archiveBuffer_ || archiveBuffer_->size() < hdr.payloadSize) {
        archiveBuffer_.reset(new Buffer( eckit::round(hdr.payloadSize, 4*1024) ));
    }

    ASSERT(hdr.payloadSize > 0);
    socket_.read(*archiveBuffer_, hdr.payloadSize);

    MemoryStream keyStream(*archiveBuffer_);
    fdb5::Key key(keyStream);

    Log::status() << "Queueing: " << key << std::endl;
    Log::debug<LibFdb>() << "Queueing: " << key << std::endl;

    size_t pos = keyStream.position();
    size_t len = hdr.payloadSize - pos;


    try {
        if (useArchiveWorker_) {
            archiveWorker_.enqueue(key, &(*archiveBuffer_)[pos], len);
            Log::status() << "Enqueued" << std::endl;
            Log::info() << "Enqueued" << std::endl;
        } else {
            fdb_.archive(key, &(*archiveBuffer_)[pos], len);
            Log::status() << "Archive complete" << std::endl;
            Log::info() << "Archive complete" << std::endl;
        }
    }
    catch(const eckit::Exception& e) {
        std::string what(e.what());
        MessageHeader response(Message::Error, what.length());
        socket_.write(&response, sizeof(response));
        socket_.write(what.c_str(), what.length());
        socket_.write(&EndMarker, sizeof(EndMarker));
        throw;
    }

    MessageHeader response(Message::Complete);
    socket_.write(&response, sizeof(response));
    socket_.write(&EndMarker, sizeof(EndMarker));
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


//----------------------------------------------------------------------------------------------------------------------


ArchiveWorker::ArchiveWorker(FDB& fdb) :
    fdb_(fdb),
    running_(false) {}


ArchiveWorker::~ArchiveWorker() {
    ASSERT(!running_);
}

void ArchiveWorker::enqueue(const Key& key, void* data, size_t length) {

    size_t maxSize = eckit::Resource<size_t>("fdbServerMaxQueueSize", 32);

    ensureWorker();

    Buffer buffer(length);
    ::memcpy(buffer, data, length);

    {
        std::unique_lock<std::mutex> lock(mutex_);
        while (queue_.size() >= maxSize) {
            cv_.wait(lock);
        }

        queue_.push(std::make_pair(key, std::move(buffer)));
    }

    cv_.notify_one();
}


void ArchiveWorker::ensureWorker() {

    if (!running_) {

        promise_ = std::promise<void>();

        // And set off the thread!

        thread_ = std::move(std::thread([this]{

            try {
                workerThreadLoop();
                promise_.set_value();
            } catch (...) {
                promise_.set_exception(std::current_exception());
            }

            {
                std::lock_guard<std::mutex> lock(mutex_);
                running_ = false;
            }
        }));

        running_ = true;
    }
}

void ArchiveWorker::workerThreadLoop() {

    Key key;
    Buffer buffer(0, 0);

    while (true) {

        // Pop something off the queue

        {
            std::unique_lock<std::mutex> lock(mutex_);
            while (queue_.empty()) {
                cv_.wait(lock);
            }

            key = queue_.front().first;
            std::swap(buffer, queue_.front().second);
            queue_.pop();
        }

        cv_.notify_one();

        // If this is a null event, then we're done!

        if (key.empty()) {
            ASSERT(buffer.size() == 0);

            Log::info() << "Flushing" << std::endl;
            Log::status() << "Flushing" << std::endl;
            fdb_.flush();
            Log::status() << "Flush complete" << std::endl;
            return;
        }

        // Otherwise archive the stuff!

        Log::info() << "Archive: " << key << std::endl;
        Log::status() << "Archive: " << key << std::endl;

        fdb_.archive(key, &buffer[0], buffer.size());

        Log::status() << "Archive done" << std::endl;
    }
}


void ArchiveWorker::flush() {

    if (running_) {

        // Enqueue a blank action to signal flushing

        enqueue(Key(), 0, 0);

        // Throws exception if things go pear shaped. Otherwise, returns when done!
        // exceptions handled in calling flush() function

        promise_.get_future().get();

        ASSERT(thread_.joinable());
        thread_.join();
    }
}




//----------------------------------------------------------------------------------------------------------------------

} // namespace remote
} // namespace fdb5

