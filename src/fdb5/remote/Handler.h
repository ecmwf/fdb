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
/// @author Tiago Quintino
/// @date   Apr 2018

#ifndef fdb5_remote_Handler_H
#define fdb5_remote_Handler_H


#include "eckit/thread/Thread.h"
#include "eckit/runtime/ProcessControler.h"
#include "eckit/io/Buffer.h"
#include "eckit/net/TCPSocket.h"
#include "eckit/memory/ScopedPtr.h"

#include "fdb5/api/FDB.h"

namespace fdb5 {

class Config;

namespace remote {

class MessageHeader;

//----------------------------------------------------------------------------------------------------------------------


class RemoteHandler : public eckit::NonCopyable {

public: // methods

    RemoteHandler(eckit::TCPSocket& socket, const Config& config = fdb5::Config());
    ~RemoteHandler();

    void handle();

private: // methods

    void flush(const MessageHeader& hdr);
    void archive(const MessageHeader& hdr);
    void retrieve(const MessageHeader& hdr);

private: // members

    eckit::TCPSocket socket_;

    eckit::ScopedPtr<eckit::Buffer> archiveBuffer_;
    eckit::ScopedPtr<eckit::Buffer> retrieveBuffer_;

    FDB fdb_;
};


//----------------------------------------------------------------------------------------------------------------------

class RemoteHandlerProcessController : public RemoteHandler
                                     , public eckit::ProcessControler {

public: // methods

    RemoteHandlerProcessController(eckit::TCPSocket& socket, const Config& config = fdb5::Config());

private: // methods

    virtual void run();
};


//----------------------------------------------------------------------------------------------------------------------


} // namespace remote
} // namespace fdb5

#endif // fdb5_remote_Handler_H
