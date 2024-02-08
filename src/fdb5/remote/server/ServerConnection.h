

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
#include <unordered_map>

#include "eckit/io/Buffer.h"
#include "eckit/io/DataHandle.h"
#include "eckit/net/TCPServer.h"
#include "eckit/net/TCPSocket.h"
#include "eckit/runtime/SessionID.h"

#include "fdb5/config/Config.h"
#include "fdb5/remote/Messages.h"
// #include "fdb5/remote/server/Handler.h"

namespace fdb5::remote {

class MessageHeader;

//----------------------------------------------------------------------------------------------------------------------
// Base class for CatalogueHandler and StoreHandler

struct readLocationElem {
    uint32_t clientID;
    uint32_t requestID;
    std::unique_ptr<eckit::DataHandle> readLocation;

    readLocationElem() : clientID(0), requestID(0), readLocation(nullptr) {}
    
    readLocationElem(uint32_t clientID, uint32_t requestID, std::unique_ptr<eckit::DataHandle> readLocation) :
        clientID(clientID), requestID(requestID), readLocation(std::move(readLocation)) {}
};

class ServerConnection : private eckit::NonCopyable {
public:  // methods
    ServerConnection(eckit::net::TCPSocket& socket, const Config& config);
    ~ServerConnection();

    virtual void handle() { NOTIMP; }

    std::string host() const { return controlSocket_.localHost(); }
    int port() const { return controlSocket_.localPort(); }
    const eckit::LocalConfiguration& agreedConf() const { return agreedConf_; }

protected:

    // Handler& handler(uint32_t id);
    // virtual Handler& handler(uint32_t id, Buffer& payload) = 0;

    // socket methods
    int selectDataPort();
    virtual void initialiseConnections();
    eckit::LocalConfiguration availableFunctionality() const;
    
    void socketRead(void* data, size_t length, eckit::net::TCPSocket& socket);

    // dataWrite is protected using a mutex, as we may have multiple workers.
    void dataWrite(Message msg, uint32_t clientID, uint32_t requestID, const void* payload = nullptr, uint32_t payloadLength = 0);
    eckit::Buffer receivePayload(const MessageHeader& hdr, eckit::net::TCPSocket& socket);

    // socket methods
    void dataWriteUnsafe(const void* data, size_t length);

    // Worker functionality
    void tidyWorkers();
    void waitForWorkers();

    void archive(const MessageHeader& hdr);
    virtual size_t archiveThreadLoop() = 0;

private:

    void controlWrite(Message msg, uint32_t clientID, uint32_t requestID, const void* payload = nullptr, uint32_t payloadLength = 0);
    void controlWrite(const void* data, size_t length);

    // virtual void read(const MessageHeader& hdr);
    // virtual void archive(const MessageHeader& hdr);
    // // virtual void store(const MessageHeader& hdr);
    // virtual void flush(const MessageHeader& hdr);

protected:

    // std::map<uint32_t, Handler*> handlers_;
    Config config_;
    eckit::net::TCPSocket controlSocket_;
    eckit::net::EphemeralTCPServer dataSocket_;
    std::string dataListenHostname_;

    eckit::Queue<readLocationElem> readLocationQueue_;

    eckit::SessionID sessionID_;
    eckit::LocalConfiguration agreedConf_;
    std::mutex controlWriteMutex_;
    std::mutex dataWriteMutex_;
    std::thread readLocationWorker_;
    
    std::map<uint32_t, std::future<void>> workerThreads_;
    std::future<size_t> archiveFuture_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
