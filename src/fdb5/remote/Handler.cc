/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Log.h"
#include "eckit/log/Bytes.h"
#include "eckit/io/Buffer.h"
#include "eckit/serialisation/MemoryStream.h"

#include "fdb5/remote/Handler.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/database/Key.h"

#include <unistd.h>


using namespace eckit;

namespace fdb5 {
namespace remote {

//----------------------------------------------------------------------------------------------------------------------

// template <typename BaseClass>
// RemoteHandler<BaseClass>::RemoteHandler(eckit::TCPSocket& socket) :
RemoteHandler::RemoteHandler(eckit::TCPSocket& socket, const Config& config) :
//    BaseClass(),
    socket_(socket),
    fdb_(config) {}


//template<typename BaseClass>
//void RemoteHandler<BaseClass>::run() {
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
            // TODO: Flush
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
            throw SeriousBug(ss.str(), Here());
        }
        };

        ASSERT(socket_.read(&tail, 4));
        ASSERT(tail == EndMarker);
    }
}

void RemoteHandler::flush(const MessageHeader& hdr) {
    Log::status() << "Flushing data" << std::endl;
    fdb_.flush();
}

void RemoteHandler::archive(const MessageHeader& hdr) {
    Log::status() << "Archiving" << std::endl;

    if (!archiveBuffer_ || archiveBuffer_->size() < hdr.payloadSize) {
        archiveBuffer_.reset(new Buffer(hdr.payloadSize + 4096 - (((hdr.payloadSize - 1) % 4096) + 1)));
    }

    ASSERT(hdr.payloadSize > 0);
    socket_.read(*archiveBuffer_, hdr.payloadSize);

    MemoryStream keyStream(*archiveBuffer_);
    fdb5::Key key(keyStream);

    Log::status() << "Archiving: " << key << std::endl;

    size_t pos = keyStream.position();
    size_t len = hdr.payloadSize - pos;
    fdb_.archive(key, &(*archiveBuffer_)[pos], len);
}

void RemoteHandler::retrieve(const MessageHeader& hdr) {
    NOTIMP;
}


RemoteHandlerProcessController::RemoteHandlerProcessController(eckit::TCPSocket& socket, const Config& config) :
    RemoteHandler(socket, config) {}


void RemoteHandlerProcessController::run() {
    handle();
}

//----------------------------------------------------------------------------------------------------------------------

// Explicit instantiations

//template class RemoteHandler<eckit::Thread>;
//template class RemoteHandler<eckit::ProcessControler>;

//----------------------------------------------------------------------------------------------------------------------

} // namespace remote
} // namespace fdb5

