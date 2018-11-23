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

#include "fdb5/api/FDB.h"
#include "fdb5/api/FDBFactory.h"
#include "fdb5/remote/Messages.h"

#include "eckit/net/TCPClient.h"
#include "eckit/net/TCPStream.h"
#include "eckit/io/Buffer.h"
#include "eckit/memory/ScopedPtr.h"
#include "eckit/container/Queue.h"


namespace fdb5 {

class FDB;

//----------------------------------------------------------------------------------------------------------------------


class RemoteFDB : public FDBBase {

public: // types

    using StoredMessage = std::pair<remote::MessageHeader, eckit::Buffer>;
    using MessageQueue = eckit::Queue<StoredMessage>;

public: // method

    RemoteFDB(const eckit::Configuration& config, const std::string& name);
    ~RemoteFDB() override;

    /// Archive writes data into aggregation buffer
    void archive(const Key& key, const void* data, size_t length) override;

    eckit::DataHandle* retrieve(const MarsRequest& request) override;

    ListIterator list(const FDBToolRequest& request) override;

    DumpIterator dump(const FDBToolRequest& request, bool simple) override;

    WhereIterator where(const FDBToolRequest& request) override;

    WipeIterator wipe(const FDBToolRequest& request, bool doit) override;

    PurgeIterator purge(const FDBToolRequest& request, bool doit) override;

    StatsIterator stats(const FDBToolRequest& request) override;

    void flush() override;

private: // methods

    void connect();
    void disconnect();

    // Listen to the dataClient for incoming messages, and push them onto
    // appropriate queues.
    void listeningThreadLoop();

    void controlWriteCheckResponse(remote::Message msg, uint32_t requestID, const void* payload=nullptr, uint32_t payloadLength=0);
    void controlWrite(remote::Message msg, uint32_t requestID, const void* payload=nullptr, uint32_t payloadLength=0);
    void controlWrite(const void* data, size_t length);
    void controlRead(void* data, size_t length);
    void dataWrite(const void* data, size_t length);
    void dataRead(void* data, size_t length);
    void handleError(const remote::MessageHeader& hdr);

    // Worker for the API functions

    template <typename HelperClass>
    auto forwardApiCall(const HelperClass& helper, const FDBToolRequest& request) -> APIIterator<typename HelperClass::ValueType>;

    // Workers for archiving

    FDBStats archiveThreadLoop(uint32_t requestID);

    void sendArchiveData(uint32_t id, const Key& key, const void* data, size_t length);

    virtual void print(std::ostream& s) const override;

    virtual FDBStats stats() const override;

private: // members

    std::string hostname_;
    int port_;
    int dataport_;
//
    eckit::TCPClient controlClient_;
    eckit::TCPClient dataClient_;

    FDBStats internalStats_;

    // Listen on the dataClient for incoming messages.
    std::thread listeningThread_;

    // Where do we put received messages

    std::map<uint32_t, MessageQueue> messageQueues_;

    // Asynchronised helpers for archiving

    friend class FDBRemoteDataHandle;

    std::future<FDBStats> archiveFuture_;

    // Helpers for retrievals

    eckit::Queue<std::pair<fdb5::Key, eckit::Buffer>> archiveQueue_;
    MessageQueue retrieveMessageQueue_;

    bool connected_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif // fdb5_remote_RemoteFDB_H
