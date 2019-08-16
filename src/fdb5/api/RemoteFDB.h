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
/// @date   Mar 2018

#ifndef fdb5_remote_RemoteFDB_H
#define fdb5_remote_RemoteFDB_H

#include <future>
#include <thread>

#include "eckit/container/Queue.h"
#include "eckit/io/Buffer.h"
#include "eckit/net/Endpoint.h"
#include "eckit/net/TCPClient.h"
#include "eckit/net/TCPStream.h"
#include "eckit/runtime/SessionID.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/FDBFactory.h"
#include "fdb5/remote/Messages.h"

namespace fdb5 {

class FDB;

//----------------------------------------------------------------------------------------------------------------------


class RemoteFDB : public FDBBase {

public: // types

    using StoredMessage = std::pair<remote::MessageHeader, eckit::Buffer>;
    using MessageQueue = eckit::Queue<StoredMessage>;
    using ArchiveQueue = eckit::Queue<std::pair<fdb5::Key, eckit::Buffer>>;

public: // method

    using FDBBase::stats;

    RemoteFDB(const eckit::Configuration& config, const std::string& name);
    ~RemoteFDB() override;

    /// Archive writes data into aggregation buffer
    void archive(const Key& key, const void* data, size_t length) override;

    eckit::DataHandle* retrieve(const metkit::MarsRequest& request) override;

    ListIterator list(const FDBToolRequest& request) override;

    DumpIterator dump(const FDBToolRequest& request, bool simple) override;

    StatusIterator status(const FDBToolRequest& request) override;

    WipeIterator wipe(const FDBToolRequest& request, bool doit, bool porcelain, bool unsafeWipeAll) override;

    PurgeIterator purge(const FDBToolRequest& request, bool doit, bool porcelain) override;

    StatsIterator stats(const FDBToolRequest& request) override;

    ControlIterator control(const FDBToolRequest& request,
                            ControlAction action,
                            ControlIdentifiers identifiers) override;

    void flush() override;

private: // methods

    // Methods to control the connection

    void connect();
    void disconnect();

    // Session negotiation with the server
    void writeControlStartupMessage();
    eckit::SessionID verifyServerStartupResponse();
    void writeDataStartupMessage(const eckit::SessionID& serverSession);

    // Construct dictionary for protocol negotiation

    eckit::LocalConfiguration availableFunctionality() const;

    // Listen to the dataClient for incoming messages, and push them onto
    // appropriate queues.
    void listeningThreadLoop();

    // Handle data going in either direction on the wire
    void controlWriteCheckResponse(remote::Message msg, uint32_t requestID, const void* payload=nullptr, uint32_t payloadLength=0);
    void controlWrite(remote::Message msg, uint32_t requestID, const void* payload=nullptr, uint32_t payloadLength=0);
    void controlWrite(const void* data, size_t length);
    void controlRead(void* data, size_t length);
    void dataWrite(remote::Message msg, uint32_t requestID, const void* payload=nullptr, uint32_t payloadLength=0);
    void dataWrite(const void* data, size_t length);
    void dataRead(void* data, size_t length);
    void handleError(const remote::MessageHeader& hdr);

    // Worker for the API functions

    template <typename HelperClass>
    auto forwardApiCall(const HelperClass& helper, const FDBToolRequest& request) -> APIIterator<typename HelperClass::ValueType>;

    // Workers for archiving

    FDBStats archiveThreadLoop(uint32_t requestID);

    void sendArchiveData(uint32_t id, const Key& key, const void* data, size_t length);
    long sendArchiveData(uint32_t id, const std::vector<std::pair<Key, eckit::Buffer>>& elements, size_t count);

    virtual void print(std::ostream& s) const override;

    virtual FDBStats stats() const override;

private: // members

    eckit::SessionID sessionID_;

    eckit::Endpoint controlEndpoint_;
    eckit::Endpoint dataEndpoint_;

    eckit::TCPClient controlClient_;
    eckit::TCPClient dataClient_;

    FDBStats internalStats_;

    // Listen on the dataClient for incoming messages.
    std::thread listeningThread_;

    // Where do we put received messages
    // @note This is a map of requestID:MessageQueue. At the point that a request is
    // complete, errored or otherwise killed, it needs to be removed from the map.
    // The shared_ptr allows this removal to be asynchronous with the actual task
    // cleaning up and returning to the client.

    std::map<uint32_t, std::shared_ptr<MessageQueue>> messageQueues_;

    // Asynchronised helpers for archiving

    friend class FDBRemoteDataHandle;

    std::future<FDBStats> archiveFuture_;

    // Helpers for retrievals

    uint32_t archiveID_;
    size_t maxArchiveQueueLength_;
    size_t maxArchiveBatchSize_;
    std::mutex archiveQueuePtrMutex_;
    std::unique_ptr<ArchiveQueue> archiveQueue_;
    MessageQueue retrieveMessageQueue_;

    std::map<uint32_t, eckit::Length> expectedSizes_;

    bool connected_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif // fdb5_remote_RemoteFDB_H
