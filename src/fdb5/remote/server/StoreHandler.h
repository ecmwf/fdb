/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#pragma once

#include "eckit/filesystem/URI.h"
#include "fdb5/api/helpers/Callback.h"
#include "fdb5/database/Store.h"
#include "fdb5/database/WipeState.h"
#include "fdb5/remote/server/ServerConnection.h"

#include <cstdint>
#include <map>
#include <memory>

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------
class StoreHandler : public ServerConnection, public CallbackRegistry {

    struct WipeInProgress;

public:  // methods

    StoreHandler(eckit::net::TCPSocket& socket, const Config& config);

private:  // methods

    Handled handleControl(Message message, uint32_t clientID, uint32_t requestID) override;
    Handled handleControl(Message message, uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) override;

    void flush(uint32_t clientID, uint32_t requestID, const eckit::Buffer& payload);

    void read(uint32_t clientID, uint32_t requestID, const eckit::Buffer& payload);

    void exists(uint32_t clientID, uint32_t requestID, const eckit::Buffer& payload) const;

    void archiveBlob(uint32_t clientID, uint32_t requestID, const void* data, size_t length) override;

    void readLocationThreadLoop();

    void writeToParent(uint32_t clientID, uint32_t requestID, std::unique_ptr<eckit::DataHandle> dh);

    bool remove(bool control, uint32_t clientID) override;

    void finaliseWipeState(const uint32_t clientID, const uint32_t requestID, const eckit::Buffer& payload);
    void doWipeUnknowns(const uint32_t clientID, const uint32_t requestID, const eckit::Buffer& payload);
    void doWipeURIs(const uint32_t clientID, const uint32_t requestID, const eckit::Buffer& payload);
    void doWipeFinish(const uint32_t clientID, const uint32_t requestID, const eckit::Buffer& payload);
    void doUnsafeFullWipe(const uint32_t clientID, const uint32_t requestID, const eckit::Buffer& payload);

    const WipeInProgress& cachedWipeState(const Key& uri) const;

    Store& getStore(uint32_t clientID);
    Store& store(uint32_t clientID, const Key& dbKey);
    Store& getStore(uint32_t clientID, const eckit::URI& uri);

private:  // members

    struct StoreHelper;
    // clientID --> Store
    std::map<uint32_t, StoreHelper> stores_;

    struct WipeInProgress {
        bool unsafeWipeAll = false;
        std::unique_ptr<StoreWipeState> state;
    };

    std::map<Key, WipeInProgress> wipesInProgress_;
};

//----------------------------------------------------------------------------------------------------------------------

struct StoreHandler::StoreHelper {
    StoreHelper(bool dataConnection, const Key& dbKey, const Config& config) :
        dataConnection(dataConnection), store(StoreFactory::instance().build(dbKey, config)) {}

    StoreHelper(bool dataConnection, const eckit::URI& uri, const Config& config) :
        dataConnection(dataConnection), store(StoreFactory::instance().build(uri, config)) {}

    bool controlConnection{true};
    bool dataConnection{false};

    std::unique_ptr<Store> store;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
