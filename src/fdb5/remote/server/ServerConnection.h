

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

#include "eckit/container/Queue.h"
#include "eckit/io/Buffer.h"
#include "eckit/io/DataHandle.h"
#include "eckit/net/TCPServer.h"
#include "eckit/net/TCPSocket.h"
#include "eckit/runtime/SessionID.h"

#include "fdb5/config/Config.h"
#include "fdb5/remote/Connection.h"
#include "fdb5/remote/Messages.h"

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

    virtual Handled handleControl(Message message, uint32_t clientID, uint32_t requestID)                          = 0;
    virtual Handled handleControl(Message message, uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) = 0;
    virtual Handled handleData(Message message, uint32_t clientID, uint32_t requestID)                             = 0;
    virtual Handled handleData(Message message, uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload)    = 0;

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
    ~ServerConnection() override;

    void initialiseConnections();

    void handle();

    std::string host() const { return controlSocket_.localHost(); }
    int port() const { return controlSocket_.localPort(); }
    const eckit::LocalConfiguration& agreedConf() const { return agreedConf_; }

    Handled handleData(Message message, uint32_t clientID, uint32_t requestID) override;
    Handled handleData(Message message, uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) override;

protected:

    // socket methods
    int selectDataPort();
    eckit::LocalConfiguration availableFunctionality() const;

    // Worker functionality
    void tidyWorkers();
    void waitForWorkers();

    // archival thread
    size_t archiveThreadLoop();

    virtual void archiveBlob(uint32_t clientID, uint32_t requestID, const void* data, size_t length) = 0;

    // archival helper methods
    void archiver();
    void queue(Message message, uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload);

    void handleException(std::exception_ptr e) override;

private:

    void listeningThreadLoopData();

    const eckit::net::TCPSocket& controlSocket() const override { return controlSocket_; }

    const eckit::net::TCPSocket& dataSocket() const override {
        ASSERT(dataSocket_);
        return *dataSocket_;
    }

protected:

    virtual bool remove(bool control, uint32_t clientID) = 0;

    Config config_;
    std::string dataListenHostname_;

    eckit::Queue<readLocationElem> readLocationQueue_;

    eckit::SessionID sessionID_;
    eckit::LocalConfiguration agreedConf_;
    std::mutex readLocationMutex_;
    std::thread readLocationWorker_;

    std::map<uint32_t, std::future<void>> workerThreads_;
    eckit::Queue<ArchiveElem> archiveQueue_;
    std::future<size_t> archiveFuture_;

    eckit::net::TCPSocket controlSocket_;

    std::mutex handlerMutex_;
    size_t numControlConnection_{0};
    size_t numDataConnection_{0};

private:

    std::mutex dataPortMutex_;

    // data connection
    std::unique_ptr<eckit::net::EphemeralTCPServer> dataSocket_;

    size_t numDataListener_{0};
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
