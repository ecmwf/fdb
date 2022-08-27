/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @author Simon Smart
/// @author Tiago Quintino
/// @date   Apr 2018

#ifndef fdb5_remote_Handler_H
#define fdb5_remote_Handler_H

#include <future>
#include <mutex>

#include "eckit/io/Buffer.h"
#include "eckit/io/DataHandle.h"
#include "eckit/net/TCPServer.h"
#include "eckit/net/TCPSocket.h"
#include "eckit/runtime/SessionID.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/api/FDB.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"
#include "fdb5/remote/Messages.h"

namespace fdb5 {

class Config;

namespace remote {

struct MessageHeader;

//----------------------------------------------------------------------------------------------------------------------

class RemoteHandler : private eckit::NonCopyable {
public:  // methods
    RemoteHandler(eckit::net::TCPSocket& socket, const Config& config);
    ~RemoteHandler();

    void handle();

    std::string host() const { return controlSocket_.localHost(); }
    int port() const { return controlSocket_.localPort(); }
    const eckit::LocalConfiguration& agreedConf() const { return agreedConf_; }

private:  // methods
    // Socket methods

    int selectDataPort();
    void initialiseConnections();
    eckit::LocalConfiguration availableFunctionality() const;

    void controlWrite(Message msg, uint32_t requestID, const void* payload = nullptr,
                      uint32_t payloadLength = 0);
    void controlWrite(const void* data, size_t length);
    void socketRead(void* data, size_t length, eckit::net::TCPSocket& socket);

    // dataWrite is protected using a mutex, as we may have multiple workers.
    void dataWrite(Message msg, uint32_t requestID, const void* payload = nullptr,
                   uint32_t payloadLength = 0);
    void dataWriteUnsafe(const void* data, size_t length);

    eckit::Buffer receivePayload(const MessageHeader& hdr, eckit::net::TCPSocket& socket);

    // Worker functionality

    void tidyWorkers();
    void waitForWorkers();

    // API functionality

    template <typename HelperClass>
    void forwardApiCall(const MessageHeader& hdr);

    void list(const MessageHeader& hdr);
    void dump(const MessageHeader& hdr);

    void flush(const MessageHeader& hdr);
    void archive(const MessageHeader& hdr);
    void retrieve(const MessageHeader& hdr);
    void read(const MessageHeader& hdr);

    void writeToParent(const uint32_t requestID, std::unique_ptr<eckit::DataHandle> dh);

    size_t archiveThreadLoop(uint32_t id);
    void readLocationThreadLoop();

private:  // members
    Config config_;
    eckit::SessionID sessionID_;

    eckit::LocalConfiguration agreedConf_;

    eckit::net::TCPSocket controlSocket_;
    eckit::net::EphemeralTCPServer dataSocket_;
    std::string dataListenHostname_;
    std::mutex dataWriteMutex_;

    // API helpers

    FDB fdb_;
    std::map<uint32_t, std::future<void>> workerThreads_;

    // Archive helpers

    std::future<size_t> archiveFuture_;

    // Retrieve helpers

    std::thread readLocationWorker_;
    eckit::Queue<std::pair<uint32_t, std::unique_ptr<eckit::DataHandle>>> readLocationQueue_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace remote
}  // namespace fdb5

#endif  // fdb5_remote_Handler_H
