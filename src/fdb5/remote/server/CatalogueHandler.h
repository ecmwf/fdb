/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#ifndef fdb5_remote_CatalogueHandler_H
#define fdb5_remote_CatalogueHandler_H

#include "fdb5/remote/server/ServerConnection.h"
#include "fdb5/api/FDB.h"


namespace fdb5::remote {
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
    void archive(const MessageHeader& hdr);
    void list(const MessageHeader& hdr);
    void inspect(const MessageHeader& hdr);
    void schema(const MessageHeader& hdr);
    

    CatalogueWriter& catalogue(Key dbKey);
    size_t archiveThreadLoop();

    // API functionality
    template <typename HelperClass>
    void forwardApiCall(const MessageHeader& hdr);

private:  // member

   std::map<Key, std::unique_ptr<CatalogueWriter>> catalogues_;
//    std::unique_ptr<FDBCatalogueBase> catalogue_;
    FDB fdb_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote

#endif  // fdb5_remote_CatalogueHandler_H
