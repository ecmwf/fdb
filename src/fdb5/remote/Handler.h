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

//#include <queue>
//#include <atomic>

//#include "eckit/thread/Thread.h"
//#include "eckit/runtime/ProcessControler.h"
#include "eckit/io/Buffer.h"
#include "eckit/net/TCPSocket.h"
#include "eckit/net/TCPServer.h"
//#include "eckit/memory/ScopedPtr.h"
//#include "eckit/container/Queue.h"

#include "fdb5/api/FDB.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"
#include "fdb5/remote/Messages.h"

#include <future>
#include <mutex>

namespace fdb5 {

class Config;

namespace remote {

class MessageHeader;

//----------------------------------------------------------------------------------------------------------------------

class RemoteHandler : private eckit::NonCopyable {

public: // methods

    RemoteHandler(eckit::TCPSocket& socket, const Config& config = Config());
    ~RemoteHandler();

    void handle();

    std::string host() const { return controlSocket_.localHost(); }
    int port() const { return controlSocket_.localPort(); }

private: // methods

    // Socket methods

    void controlWrite(Message msg, uint32_t requestID, const void* payload=nullptr, uint32_t payloadLength=0);
    void controlWrite(const void* data, size_t length);
    void controlRead(void* data, size_t length);

    // dataWrite is protected using a mutex, as we may have multiple workers.
    void dataWrite(Message msg, uint32_t requestID, const void* payload=nullptr, uint32_t payloadLength=0);
    void dataWriteUnsafe(const void* data, size_t length);

    eckit::Buffer receivePayload(const MessageHeader& hdr);

    // Worker functionality

    void tidyWorkers();
    void waitForWorkers();

    // API functionality

    template <typename HelperClass>
    void forwardApiCall(const MessageHeader& hdr);

    void list(const MessageHeader& hdr);
    void dump(const MessageHeader& hdr);

//    void flush();
//    void archive(const MessageHeader& hdr);
//    void retrieve(const MessageHeader& hdr);
//
//    void archiveThreadLoop();

private: // members

    eckit::TCPSocket controlSocket_;
    eckit::TCPServer dataSocket_;

    std::map<uint32_t, std::future<void>> workerThreads_;

//
//    eckit::ScopedPtr<eckit::Buffer> archiveBuffer_;
//    eckit::ScopedPtr<eckit::Buffer> retrieveBuffer_;
//
    FDB fdb_;
//
//    eckit::Queue<std::pair<fdb5::Key, eckit::Buffer>> archiveQueue_;
//    std::future<void> archiveFuture_;

    std::mutex dataWriteMutex_;
};


//----------------------------------------------------------------------------------------------------------------------


} // namespace remote
} // namespace fdb5

#endif // fdb5_remote_Handler_H
