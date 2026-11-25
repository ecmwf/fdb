// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/WipeState.h"
#include "fdb5/remote/server/ServerConnection.h"

#include <memory>
#include <mutex>

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

struct CatalogueArchiver {
    CatalogueArchiver(const Key& dbKey, const Config& config) :
        catalogue(CatalogueWriterFactory::instance().build(dbKey, config)),
        locationsExpected(0),
        locationsArchived(0) {}

    std::unique_ptr<CatalogueWriter> catalogue;
    size_t locationsExpected;
    size_t locationsArchived;

    std::promise<size_t> fieldLocationsReceived;
    std::future<size_t> archivalCompleted;
};

//----------------------------------------------------------------------------------------------------------------------

struct WipeHelper;
class CatalogueHandler : public ServerConnection {

    friend struct WipeHelper;
    struct WipeInProgress;

public:  // methods

    CatalogueHandler(eckit::net::TCPSocket& socket, const Config& config);
    ~CatalogueHandler() override;

private:  // methods

    Handled handleControl(Message message, uint32_t clientID, uint32_t requestID) override;
    Handled handleControl(Message message, uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) override;

    // API functionality
    template <typename HelperClass>
    void handleApiCall(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload);

    void flush(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload);
    void list(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload);
    void axes(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload);
    void inspect(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload);
    void wipe(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload);
    void stats(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload);
    void schema(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload);
    void stores(uint32_t clientID, uint32_t requestID);
    void exists(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) const;

    void archiveBlob(const uint32_t clientID, const uint32_t requestID, const void* data, size_t length) override;

    void remove(uint32_t clientID) override;

    CatalogueWriter& catalogue(uint32_t catalogueID, const Key& dbKey);
    // the archiver of a client, pinned. @returns null if there is none
    std::shared_ptr<CatalogueArchiver> catalogueArchiver(uint32_t clientID);
    // the FDB of a client (for the API calls), pinned
    std::shared_ptr<FDB> fdb(uint32_t clientID);

    void doMaskIndexEntries(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload);
    void doWipeURIs(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload);
    void doWipeUnknowns(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) const;
    void doWipeEmptyDatabase(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload);
    void doUnsafeFullWipe(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload);

    const Config& config() const { return config_; }

    const WipeInProgress& cachedWipeState(Key dbKey) const;

private:  // member

    // clientID --> <catalogue, locationsExpected, locationsArchived>. Guarded by handlerMutex_; the users of an entry
    // pin it (shared_ptr), so that a Stop can remove it while a worker is still using it.
    std::map<uint32_t, std::shared_ptr<CatalogueArchiver>> catalogues_;
    std::map<uint32_t, std::shared_ptr<FDB>> fdbs_;

    std::mutex fieldLocationsMutex_;


    struct WipeInProgress {
        bool unsafeWipeAll = false;
        std::unique_ptr<CatalogueReader> catalogue;
        CatalogueWipeState state;
    };

    std::map<Key, WipeInProgress> wipesInProgress_;
    mutable std::mutex wipesMutex_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
