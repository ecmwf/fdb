// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

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
    ~StoreHandler() override;

private:  // methods

    Handled handleControl(Message message, uint32_t clientID, uint32_t requestID) override;
    Handled handleControl(Message message, uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) override;

    void flush(uint32_t clientID, uint32_t requestID, const eckit::Buffer& payload);

    void read(uint32_t clientID, uint32_t requestID, const eckit::Buffer& payload);

    void exists(uint32_t clientID, uint32_t requestID, const eckit::Buffer& payload) const;

    void archiveBlob(uint32_t clientID, uint32_t requestID, const void* data, size_t length) override;

    void readLocationThreadLoop();

    void writeToParent(uint32_t clientID, uint32_t requestID, std::unique_ptr<eckit::DataHandle> dh);

    void remove(uint32_t clientID) override;

    void finaliseWipeState(const uint32_t clientID, const uint32_t requestID, const eckit::Buffer& payload);
    void doWipeUnknowns(const uint32_t clientID, const uint32_t requestID, const eckit::Buffer& payload);
    void doWipeURIs(const uint32_t clientID, const uint32_t requestID, const eckit::Buffer& payload);
    void doWipeFinish(const uint32_t clientID, const uint32_t requestID, const eckit::Buffer& payload);
    void doUnsafeFullWipe(const uint32_t clientID, const uint32_t requestID, const eckit::Buffer& payload);

    const WipeInProgress& cachedWipeState(const Key& uri) const;

    // The store of a client, pinned: it stays alive while it is used, even if the client is removed meanwhile
    std::shared_ptr<Store> getStore(uint32_t clientID);
    // ... created if needed. @returns null if the client has been removed
    std::shared_ptr<Store> store(uint32_t clientID, const Key& dbKey);
    std::shared_ptr<Store> getStore(uint32_t clientID, const eckit::URI& uri);

    void registerFlushCallback(FlushCallback callback) override;
    void registerArchiveCallback(ArchiveCallback callback) override;

private:  // members

    struct StoreHelper;
    // clientID --> Store
    std::map<uint32_t, std::shared_ptr<StoreHelper>> stores_;  // guarded by handlerMutex_

    struct WipeInProgress {
        bool unsafeWipeAll = false;
        std::unique_ptr<StoreWipeState> state;
    };

    std::map<Key, WipeInProgress> wipesInProgress_;

    Callbacks callbacks_;
};

//----------------------------------------------------------------------------------------------------------------------

struct StoreHandler::StoreHelper {
    StoreHelper(const Key& dbKey, const Config& config) : store(StoreFactory::instance().build(dbKey, config)) {}

    StoreHelper(const eckit::URI& uri, const Config& config) : store(StoreFactory::instance().build(uri, config)) {}

    std::unique_ptr<Store> store;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
