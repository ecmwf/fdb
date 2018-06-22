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

#include <queue>
#include <mutex>
#include <thread>
#include <future>
#include <atomic>

#include "eckit/thread/Thread.h"
#include "eckit/runtime/ProcessControler.h"
#include "eckit/io/Buffer.h"
#include "eckit/net/TCPSocket.h"
#include "eckit/memory/ScopedPtr.h"
#include "eckit/container/Queue.h"

#include "fdb5/api/FDB.h"
#include "fdb5/database/Key.h"

namespace fdb5 {

class Config;

namespace remote {

class MessageHeader;

//----------------------------------------------------------------------------------------------------------------------

class RemoteHandler : private eckit::NonCopyable {

public: // methods

    RemoteHandler(eckit::TCPSocket& socket, const Config& config = fdb5::Config());
    ~RemoteHandler();

    void handle();

private: // methods

    void flush(const MessageHeader& hdr);
    void archive(const MessageHeader& hdr);
    void retrieve(const MessageHeader& hdr);

    void archiveThreadLoop();

private: // members

    eckit::TCPSocket socket_;

    eckit::ScopedPtr<eckit::Buffer> archiveBuffer_;
    eckit::ScopedPtr<eckit::Buffer> retrieveBuffer_;

    FDB fdb_;

    eckit::Queue<std::pair<fdb5::Key, eckit::Buffer>> archiveQueue_;
    std::future<void> archiveFuture_;
};


//----------------------------------------------------------------------------------------------------------------------


} // namespace remote
} // namespace fdb5

#endif // fdb5_remote_Handler_H
