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
/// @author Emanuele Danovaro
/// @author Chris Bradley
/// @date   Mar 2018

#pragma once

#include <thread>
#include <unordered_map>

#include "fdb5/api/LocalFDB.h"
#include "fdb5/remote/client/Client.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------
class Archiver;

class RemoteFDB : public LocalFDB, public remote::Client {

public:  // types

    using MessageQueue = eckit::Queue<eckit::Buffer>;

public:  // method

    RemoteFDB(const eckit::Configuration& config, const std::string& name);
    ~RemoteFDB() override {}

    ListIterator inspect(const metkit::mars::MarsRequest& request) override;

    ListIterator list(const FDBToolRequest& request, int depth) override;

    AxesIterator axesIterator(const FDBToolRequest& request, int depth = 3) override;

    DumpIterator dump(const FDBToolRequest& request, bool simple) override { NOTIMP; }

    StatusIterator status(const FDBToolRequest& request) override { NOTIMP; }

    WipeIterator wipe(const FDBToolRequest& request, bool doit, bool porcelain, bool unsafeWipeAll) override { NOTIMP; }

    PurgeIterator purge(const FDBToolRequest& request, bool doit, bool porcelain) override { NOTIMP; }

    StatsIterator stats(const FDBToolRequest& request) override;

    ControlIterator control(const FDBToolRequest& request, ControlAction action,
                            ControlIdentifiers identifiers) override {
        NOTIMP;
    }

    MoveIterator move(const FDBToolRequest& request, const eckit::URI& dest) override { NOTIMP; }

    const eckit::net::Endpoint& storeEndpoint() const;
    const eckit::net::Endpoint& storeEndpoint(const eckit::net::Endpoint& fieldLocationEndpoint) const;

private:  // methods

    template <typename HelperClass>
    auto forwardApiCall(const HelperClass& helper, const FDBToolRequest& request)
        -> APIIterator<typename HelperClass::ValueType>;

    void print(std::ostream& s) const override;

    // Client
    bool handle(remote::Message message, uint32_t requestID) override;
    bool handle(remote::Message message, uint32_t requestID, eckit::Buffer&& payload) override;

private:  // members

    std::unordered_map<eckit::net::Endpoint, eckit::net::Endpoint> storesReadMapping_;
    std::vector<std::pair<eckit::net::Endpoint, eckit::net::Endpoint>> storesArchiveMapping_;
    std::vector<eckit::net::Endpoint> storesLocalFields_;

    // Where do we put received messages
    // @note This is a map of requestID:MessageQueue. At the point that a request is
    // complete, errored or otherwise killed, it needs to be removed from the map.
    // The shared_ptr allows this removal to be asynchronous with the actual task
    // cleaning up and returning to the client.
    std::unordered_map<uint32_t, std::shared_ptr<MessageQueue>> messageQueues_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
