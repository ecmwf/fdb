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

#include "fdb5/remote/server/ServerConnection.h"
#include "fdb5/api/FDB.h"

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

struct CatalogueArchiver {
    CatalogueArchiver(const Key& dbKey, const Config& config) :
        catalogue(CatalogueWriterFactory::instance().build(dbKey, config)), locationsExpected(-1), locationsArchived(0) {
        archivalCompleted = fieldLocationsReceived.get_future();
    }

    std::unique_ptr<CatalogueWriter> catalogue;
    int32_t locationsExpected;
    int32_t locationsArchived;

    std::promise<int32_t> fieldLocationsReceived;
    std::future<int32_t> archivalCompleted;
};

//----------------------------------------------------------------------------------------------------------------------
class CatalogueHandler : public ServerConnection {
public:  // methods

    CatalogueHandler(eckit::net::TCPSocket& socket, const Config& config);
    ~CatalogueHandler();

private:  // methods

    Handled handleControl(Message message, uint32_t clientID, uint32_t requestID) override;
    Handled handleControl(Message message, uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) override;
    // Handled handleData(Message message, uint32_t clientID, uint32_t requestID) override;
    // Handled handleData(Message message, uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) override;

    // API functionality
    template <typename HelperClass>
    void forwardApiCall(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload);

    void flush(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload);
    void list(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload);
    void inspect(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload);
    void schema(uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload);
    void stores(uint32_t clientID, uint32_t requestID);

    void archiveBlob(const uint32_t clientID, const uint32_t requestID, const void* data, size_t length) override;

    bool remove(uint32_t clientID) override;

    // CatalogueWriter& catalogue(uint32_t catalogueID);
    CatalogueWriter& catalogue(uint32_t catalogueID, const Key& dbKey);

private:  // member

    std::mutex cataloguesMutex_;
    // clientID --> <catalogue, locationsExpected, locationsArchived>
    std::map<uint32_t, CatalogueArchiver> catalogues_;

    FDB fdb_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
