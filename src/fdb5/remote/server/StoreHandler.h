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

#include "fdb5/database/Store.h"
#include "fdb5/remote/server/ServerConnection.h"

namespace fdb5::remote {
    
//----------------------------------------------------------------------------------------------------------------------

struct StoreHelper {
    StoreHelper(bool dataConnection, const Key& dbKey, const Config& config) :
        controlConnection(true), dataConnection(dataConnection),
        store(StoreFactory::instance().build(dbKey, config)) {}

    bool controlConnection;
    bool dataConnection;
    
    std::unique_ptr<Store> store;
};

//----------------------------------------------------------------------------------------------------------------------
class StoreHandler : public ServerConnection {
public:  // methods

    StoreHandler(eckit::net::TCPSocket& socket, const Config& config);
    ~StoreHandler();

private:  // methods

    Handled handleControl(Message message, uint32_t clientID, uint32_t requestID) override;
    Handled handleControl(Message message, uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) override;

    void flush(uint32_t clientID, uint32_t requestID, const eckit::Buffer& payload);
    void read(uint32_t clientID, uint32_t requestID, const eckit::Buffer& payload);

    void archiveBlob(const uint32_t clientID, const uint32_t requestID, const void* data, size_t length) override;

    void readLocationThreadLoop();
    void writeToParent(const uint32_t clientID, const uint32_t requestID, std::unique_ptr<eckit::DataHandle> dh);

    bool remove(bool control, uint32_t clientID) override;
    // bool handlers() override;

    Store& store(uint32_t clientID);
    Store& store(uint32_t clientID, const Key& dbKey);

private:  // members

    // clientID --> Store
    std::map<uint32_t, StoreHelper> stores_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote