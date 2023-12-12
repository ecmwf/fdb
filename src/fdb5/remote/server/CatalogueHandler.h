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

// class StoreEndpoint : public eckit::net::Endpoint {
// public:
//     StoreEndpoint(const std::string& endpoint) : eckit::net::Endpoint(endpoint) {}
//     std::map<std::string, eckit::net::Endpoint> aliases_;
// };

//----------------------------------------------------------------------------------------------------------------------
class CatalogueHandler : public ServerConnection {
public:  // methods

    CatalogueHandler(eckit::net::TCPSocket& socket, const Config& config);
    ~CatalogueHandler();

    void handle() override;

private:  // methods

    void initialiseConnections() override;
    void index(const MessageHeader& hdr);

    void read(const MessageHeader& hdr);
    void flush(const MessageHeader& hdr);
    void list(const MessageHeader& hdr);
    void inspect(const MessageHeader& hdr);
    void schema(const MessageHeader& hdr);
    

    CatalogueWriter& catalogue(uint32_t id);
    CatalogueWriter& catalogue(uint32_t id, const Key& dbKey);

    size_t archiveThreadLoop(uint32_t archiverID) override;

    // API functionality
    template <typename HelperClass>
    void forwardApiCall(const MessageHeader& hdr);

private:  // member

    // clientID --> Catalogue
    std::map<uint32_t, std::unique_ptr<CatalogueWriter>> catalogues_;

    FDB fdb_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
