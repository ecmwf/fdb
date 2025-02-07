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

#include "fdb5/api/FDB.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/remote/server/ServerConnection.h"

#include <memory>

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

struct CatalogueArchiver {
    CatalogueArchiver(bool dataConnection, const Key& dbKey, const Config& config) :
        controlConnection(true),
        dataConnection(dataConnection),
        catalogue(CatalogueWriterFactory::instance().build(dbKey, config)),
        locationsExpected(0),
        locationsArchived(0) {}

    bool controlConnection;
    bool dataConnection;

    std::unique_ptr<CatalogueWriter> catalogue;
    size_t locationsExpected;
    size_t locationsArchived;

    std::promise<size_t> fieldLocationsReceived;
    std::future<size_t> archivalCompleted;
};

//----------------------------------------------------------------------------------------------------------------------
class CatalogueHandler : public ServerConnection {
public:  // methods

    CatalogueHandler(eckit::net::TCPSocket& socket, const Config& config);
    ~CatalogueHandler() override;

private:  // methods

    Handled handleControl(Message message, uint32_t clientID, uint32_t requestID) override;
    Handled handleControl(Message message, uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) override;

    // API functionality
    template <typename HelperClass>
    void forwardApiCall(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload);

    void flush(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload);
    void list(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload);
    void axes(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload);
    void inspect(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload);
    void stats(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload);
    void schema(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload);
    void stores(uint32_t clientID, uint32_t requestID);
    void exists(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) const;

    void archiveBlob(const uint32_t clientID, const uint32_t requestID, const void* data, size_t length) override;

    bool remove(bool control, uint32_t clientID) override;

    CatalogueWriter& catalogue(uint32_t catalogueID, const Key& dbKey);

private:  // member

    // clientID --> <catalogue, locationsExpected, locationsArchived>
    std::map<uint32_t, CatalogueArchiver> catalogues_;
    std::map<uint32_t, FDB> fdbs_;

    std::mutex fdbMutex_;
    std::mutex fieldLocationsMutex_;

    bool fdbControlConnection_;
    bool fdbDataConnection_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
