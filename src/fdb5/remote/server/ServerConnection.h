

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
#include "fdb5/remote/Connection.h"

namespace fdb5::remote {

enum class Handled {
    No = 0,
    Yes,
    YesAddArchiveListener,
    YesRemoveArchiveListener,
    YesAddReadListener,
    YesRemoveReadListener,
    Replied,
};

//----------------------------------------------------------------------------------------------------------------------

class Handler {

public:

    virtual Handled handleControl(Message message, uint32_t clientID, uint32_t requestID) = 0;
    virtual Handled handleControl(Message message, uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) = 0;
    virtual Handled handleData(Message message, uint32_t clientID, uint32_t requestID) = 0;
    virtual Handled handleData(Message message, uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) = 0;

    virtual void handleException(std::exception_ptr e) = 0;
};

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

struct ArchiveElem {

    uint32_t clientID_;
    uint32_t requestID_;
    eckit::Buffer payload_;
    bool multiblob_;

    ArchiveElem() : clientID_(0), requestID_(0), payload_(0), multiblob_(false) {}

    ArchiveElem(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload, bool multiblob) :
        clientID_(clientID), requestID_(requestID), payload_(std::move(payload)), multiblob_(multiblob) {}
};

class ServerConnection : public Connection, public Handler {
public:  // methods
    ServerConnection(eckit::net::TCPSocket& socket, const Config& config);
    ~ServerConnection();

    void initialiseConnections();

    void handle();

    std::string host() const { return controlSocket_.localHost(); }
    int port() const { return controlSocket_.localPort(); }
    const eckit::LocalConfiguration& agreedConf() const { return agreedConf_; }

    Handled handleData(Message message, uint32_t clientID, uint32_t requestID) override;
    Handled handleData(Message message, uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) override;

protected:

    // Handler& handler(uint32_t id);
    // virtual Handler& handler(uint32_t id, Buffer& payload) = 0;

    // socket methods
    int selectDataPort();
//    virtual void initialiseConnections();
    eckit::LocalConfiguration availableFunctionality() const;
    
        // Worker functionality
    void tidyWorkers();
    void waitForWorkers();

    // archival thread
    size_t archiveThreadLoop();
    virtual void archiveBlob(const uint32_t clientID, const uint32_t requestID, const void* data, size_t length) = 0;

    // archival helper methods
    void archiver();
    // emplace new ArchiveElem to archival queue
    void queue(Message message, uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload);
    // // retrieve archival queue
    // eckit::Queue<ArchiveElem>& queue();

    void handleException(std::exception_ptr e) override;

    // void archive(const MessageHeader& hdr);
    // virtual size_t archiveThreadLoop() = 0;

private:

    // void controlWrite(Message msg, uint32_t clientID, uint32_t requestID, const void* payload = nullptr, uint32_t payloadLength = 0);
    // void controlWrite(const void* data, size_t length);
    void listeningThreadLoopData();

    eckit::net::TCPSocket& controlSocket() override { return controlSocket_; }
    eckit::net::TCPSocket& dataSocket() override { 
        ASSERT(dataSocket_);
        return *dataSocket_;
    }

protected:

    virtual bool remove(uint32_t clientID) = 0;

    // std::map<uint32_t, Handler*> handlers_;
    Config config_;
    std::string dataListenHostname_;

    eckit::Queue<readLocationElem> readLocationQueue_;

    eckit::SessionID sessionID_;
    eckit::LocalConfiguration agreedConf_;
    // std::mutex controlWriteMutex_;
    // std::mutex dataWriteMutex_;
    std::thread readLocationWorker_;
    
    std::map<uint32_t, std::future<void>> workerThreads_;
    eckit::Queue<ArchiveElem> archiveQueue_;
    std::future<size_t> archiveFuture_;

    eckit::net::TCPSocket controlSocket_;

private:

    // data connection
    std::unique_ptr<eckit::net::EphemeralTCPServer> dataSocket_;
    size_t dataListener_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
