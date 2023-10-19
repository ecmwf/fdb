/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   RemoteStore.h
/// @author Emanuele Danovaro
/// @date   October 2023

#pragma once

#include "fdb5/api/FDBStats.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Store.h"
#include "fdb5/remote/client/Client.h"

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

/// Store that connects to a remote Store

class RemoteStore : public Store, public Client {

public: // types

    using ArchiveQueue = eckit::Queue<std::pair<uint32_t, std::pair<fdb5::Key, eckit::Buffer> > >;
    using StoredMessage = std::pair<remote::MessageHeader, eckit::Buffer>;
    using MessageQueue = eckit::Queue<StoredMessage>;

public: // methods

    RemoteStore(const Key& key, const Config& config);
//    RemoteStore(const Key& key, const Config& config, const eckit::net::Endpoint& controlEndpoint);
    RemoteStore(const eckit::URI& uri, const Config& config);

    ~RemoteStore() override;

    eckit::URI uri() const override;

    bool open() override;
    void flush() override;
    void close() override;

    void checkUID() const override { }

    eckit::DataHandle* dataHandle(const FieldLocation& fieldLocation);
    eckit::DataHandle* dataHandle(const FieldLocation& fieldLocation, const Key& remapKey);

    bool canMoveTo(const Key& key, const Config& config, const eckit::URI& dest) const override { return false; }
    void moveTo(const Key& key, const Config& config, const eckit::URI& dest, eckit::Queue<MoveElement>& queue) const override { NOTIMP; }
    void remove(const Key& key) const override;

   const Config& config() { return config_; }
   
protected: // methods

    std::string type() const override { return "fdbremote"; }

    bool exists() const override;

    eckit::DataHandle* retrieve(Field& field) const override;
    std::future<std::unique_ptr<FieldLocation> > archive(const Key& key, const void *data, eckit::Length length) override;

    void remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const override;

    void print( std::ostream &out ) const override;

private: // methods

    Key key() override { return dbKey_; }

    // handlers for incoming messages - to be defined in the client class
    bool handle(Message message, uint32_t requestID) override;
    bool handle(Message message, uint32_t requestID, eckit::net::Endpoint endpoint, eckit::Buffer&& payload) override;
    void handleException(std::exception_ptr e) override;

    FDBStats archiveThreadLoop();
    // Listen to the dataClient for incoming messages, and push them onto
    // appropriate queues.
//    void listeningThreadLoop() override;
//    long sendArchiveData(uint32_t id, const std::vector<std::pair<Key, eckit::Buffer>>& elements, size_t count);
    void sendArchiveData(uint32_t id, const Key& key, const void* data, size_t length);
    void flush(FDBStats& stats);

//     void connect();
//     void disconnect();

private: // members

    Key dbKey_;
    const Config& config_;

    bool dirty_;
    std::future<FDBStats> archiveFuture_;
//    uint32_t archiveID_;
    std::mutex archiveQueuePtrMutex_;
    std::unique_ptr<ArchiveQueue> archiveQueue_;
    size_t maxArchiveQueueLength_;
    size_t maxArchiveBatchSize_;
    // @note This is a map of requestID:MessageQueue. At the point that a request is
    // complete, errored or otherwise killed, it needs to be removed from the map.
    // The shared_ptr allows this removal to be asynchronous with the actual task
    // cleaning up and returning to the client.

    std::map<uint32_t, std::shared_ptr<MessageQueue>> messageQueues_;
    MessageQueue retrieveMessageQueue_;

    std::map<uint32_t, std::promise<std::unique_ptr<FieldLocation> > > locations_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5::remote
