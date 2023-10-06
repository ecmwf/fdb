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

#include "fdb5/remote/DecoupledHandler.h"

namespace fdb5 {
namespace remote {
//----------------------------------------------------------------------------------------------------------------------
class CatalogueHandler : public DecoupledHandler {
public:  // methods

    CatalogueHandler(eckit::net::TCPSocket& socket, const Config& config);
    ~CatalogueHandler();

    void handle() override;

private:  // methods

    void initialiseConnections() override;
    void index(const MessageHeader& hdr);

    // API functionality
    template <typename HelperClass>
    void forwardApiCall(const MessageHeader& hdr);

private:  // member

//    std::unique_ptr<FDBCatalogueBase> catalogue_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace remote
}  // namespace fdb5

#endif  // fdb5_remote_CatalogueHandler_H
