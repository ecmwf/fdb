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

namespace fdb5::remote {
    
//----------------------------------------------------------------------------------------------------------------------
class StoreHandler : public ServerConnection {
public:  // methods
    StoreHandler(eckit::net::TCPSocket& socket, const Config& config);
    ~StoreHandler();

    void handle() override;

private:  // methods

    void read(const MessageHeader& hdr);

    void initialiseConnections() override;
    void readLocationThreadLoop();
    void writeToParent(const uint32_t requestID, std::unique_ptr<eckit::DataHandle> dh);

    void archive(const MessageHeader& hdr);
    size_t archiveThreadLoop();
    void archiveBlobPayload(uint32_t id, const void* data, size_t length);

    void flush(const MessageHeader& hdr);

  //  Catalogue& catalogue(Key dbKey);
    Store& store(Key dbKey);
  //  Store& store(eckit::URI uri);

private:  // members
//    std::map<Key, std::unique_ptr<Catalogue>> catalogues_;
    std::map<Key, std::unique_ptr<Store>> stores_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
