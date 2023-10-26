

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

#pragma once

#include <future>
#include <mutex>

#include "eckit/io/Buffer.h"
#include "eckit/io/DataHandle.h"
#include "eckit/net/TCPServer.h"
#include "eckit/net/TCPSocket.h"
#include "eckit/runtime/SessionID.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/api/FDBFactory.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"
#include "fdb5/remote/Messages.h"

namespace fdb5 {

class Config;

namespace remote {

struct MessageHeader;

//----------------------------------------------------------------------------------------------------------------------
// Base class for CatalogueHandler and StoreHandler

class ServerConnection : private eckit::NonCopyable {
public:  // methods
    ServerConnection(eckit::net::TCPSocket& socket, const Config& config);
    ~ServerConnection();

    virtual void handle() { NOTIMP; }

    std::string host() const { return controlSocket_.localHost(); }
    int port() const { return controlSocket_.localPort(); }
    const eckit::LocalConfiguration& agreedConf() const { return agreedConf_; }

protected:

    // socket methods
    int selectDataPort();
    virtual void initialiseConnections();
    eckit::LocalConfiguration availableFunctionality() const;

    void controlWrite(Message msg, uint32_t requestID, const void* payload = nullptr, uint32_t payloadLength = 0);
    void controlWrite(const void* data, size_t length);
    void socketRead(void* data, size_t length, eckit::net::TCPSocket& socket);
    
    // dataWrite is protected using a mutex, as we may have multiple workers.
    void dataWrite(Message msg, uint32_t requestID, const void* payload = nullptr, uint32_t payloadLength = 0);
    eckit::Buffer receivePayload(const MessageHeader& hdr, eckit::net::TCPSocket& socket);

    // socket methods
    void dataWriteUnsafe(const void* data, size_t length);

    // Worker functionality
    void tidyWorkers();
    void waitForWorkers();


    // virtual void read(const MessageHeader& hdr);
    // virtual void archive(const MessageHeader& hdr);
    // // virtual void store(const MessageHeader& hdr);
    // virtual void flush(const MessageHeader& hdr);

protected:

    Config config_;
    eckit::net::TCPSocket controlSocket_;
    eckit::net::EphemeralTCPServer dataSocket_;
    std::string dataListenHostname_;
    // FDB fdb_;
    eckit::Queue<std::pair<uint32_t, std::unique_ptr<eckit::DataHandle>>> readLocationQueue_;

    eckit::SessionID sessionID_;
    eckit::LocalConfiguration agreedConf_;
    std::mutex dataWriteMutex_;
    std::map<uint32_t, std::future<void>> workerThreads_;
    std::thread readLocationWorker_;
    
    std::future<size_t> archiveFuture_;

};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace remote
}  // namespace fdb5


//----------------------------------------------------------------------------------------------------------------------

