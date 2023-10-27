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
/// @date   Mar 2018

#ifndef fdb5_remote_RemoteFDB_H
#define fdb5_remote_RemoteFDB_H

#include <future>
#include <thread>

#include "fdb5/api/FDB.h"
#include "fdb5/api/FDBFactory.h"
#include "fdb5/remote/client/Client.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------
class Archiver;

class RemoteFDB : public FDBBase, public remote::Client {

public: // types

public: // method

    using FDBBase::stats;

    RemoteFDB(const eckit::Configuration& config, const std::string& name);

    /// Archive writes data into aggregation buffer
    void archive(const Key& key, const void* data, size_t length) override;

    ListIterator inspect(const metkit::mars::MarsRequest& request) override;

    ListIterator list(const FDBToolRequest& request) override;

    DumpIterator dump(const FDBToolRequest& request, bool simple) override;

    StatusIterator status(const FDBToolRequest& request) override;

    WipeIterator wipe(const FDBToolRequest& request, bool doit, bool porcelain, bool unsafeWipeAll) override;

    PurgeIterator purge(const FDBToolRequest& request, bool doit, bool porcelain) override;

    StatsIterator stats(const FDBToolRequest& request) override;

    ControlIterator control(const FDBToolRequest& request,
                            ControlAction action,
                            ControlIdentifiers identifiers) override;

    MoveIterator move(const FDBToolRequest& request, const eckit::URI& dest) override;

    void flush() override;

    // Client

    bool handle(remote::Message message, uint32_t requestID) override;
    bool handle(remote::Message message, uint32_t requestID, eckit::net::Endpoint endpoint, eckit::Buffer&& payload) override;
    void handleException(std::exception_ptr e) override;

    const Key& key() const override;

private: // methods

    virtual void print(std::ostream& s) const override;

    virtual FDBStats stats() const override;

private: // members

    std::unique_ptr<Archiver> archiver_;

    bool dolist_ = false;
    eckit::Queue<ListElement>* listqueue_;

    bool doinspect_ = false;
    eckit::Queue<ListElement>* inspectqueue_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif // fdb5_remote_RemoteFDB_H