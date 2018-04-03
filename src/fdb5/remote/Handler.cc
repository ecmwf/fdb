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

#include "fdb5/remote/Handler.h"
#include "fdb5/remote/Messages.h"

#include <unistd.h>


using namespace eckit;

namespace fdb5 {
namespace remote {

//----------------------------------------------------------------------------------------------------------------------

// template <typename BaseClass>
// RemoteHandler<BaseClass>::RemoteHandler(eckit::TCPSocket& socket) :
RemoteHandler::RemoteHandler(eckit::TCPSocket& socket) :
//    BaseClass(),
    socket_(socket) {}


//template<typename BaseClass>
//void RemoteHandler<BaseClass>::run() {
void RemoteHandler::handle() {

    Log::info() << "... started" << std::endl;

    MessageHeader hdr;

    while (socket_.read(&hdr, sizeof(hdr))) {

        ASSERT(hdr.marker == StartMarker);
        ASSERT(hdr.version == CurrentVersion);

        if (hdr.message == Message::Exit) {
            Log::status() << "Exit message recieved" << std::endl;
            Log::info() << "Exit message recieved" << std::endl;
            break;
        }

        if (hdr.message == Message::Flush) {
            Log::status() << "Flush message recieved" << std::endl;
            Log::info() << "Flush message recieved" << std::endl;
        }

        Log::info() << "Sleeping" << std::endl;
        usleep(4000000);
        Log::info() << "Sleeped" << std::endl;
    }
}


RemoteHandlerProcessController::RemoteHandlerProcessController(eckit::TCPSocket& socket) :
    RemoteHandler(socket) {}


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

