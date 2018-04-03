/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @date   Apr 2018

#ifndef fdb5_remote_Handler_H
#define fdb5_remote_Handler_H


#include "eckit/thread/Thread.h"
#include "eckit/runtime/ProcessControler.h"
#include "eckit/net/TCPSocket.h"


namespace fdb5 {
namespace remote {

//----------------------------------------------------------------------------------------------------------------------

class RemoteHandler : public eckit::NonCopyable {

public: // methods

    RemoteHandler(eckit::TCPSocket& socket);

    void handle();

private: // members

    eckit::TCPSocket socket_;
};


//template <typename BaseClass>
//class RemoteHandler : public BaseClass {
//
//public: // methods
//
//    RemoteHandler(eckit::TCPSocket& socket);
//
//private: // Here we go!
//
//    virtual void run();
//
//private: // members
//
//    eckit::TCPSocket socket_;
//};
//
//extern template class RemoteHandler<eckit::Thread>;
//extern template class RemoteHandler<eckit::ProcessControler>;
//
//typedef RemoteHandler<eckit::Thread> RemoteHandlerThread;
//typedef RemoteHandler<eckit::ProcessControler> RemoteHandlerProcessControler;

//----------------------------------------------------------------------------------------------------------------------

class RemoteHandlerProcessController : public RemoteHandler
                                     , public eckit::ProcessControler {

public: // methods

    RemoteHandlerProcessController(eckit::TCPSocket& socket);

private: // methods

    virtual void run();
};


//----------------------------------------------------------------------------------------------------------------------


} // namespace remote
} // namespace fdb5

#endif // fdb5_remote_Handler_H
