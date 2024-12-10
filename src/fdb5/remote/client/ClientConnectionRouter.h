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

#include "fdb5/remote/client/Client.h"
#include "fdb5/remote/client/ClientConnection.h"

#include <unordered_map>

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

class ClientConnectionRouter : eckit::NonCopyable {
public:

    static ClientConnectionRouter& instance();

    ClientConnection& connection(const eckit::net::Endpoint& endpoint, const std::string& defaultEndpoint);
    ClientConnection& connection(const std::vector<std::pair<eckit::net::Endpoint, std::string>>& endpoints);

    void teardown(std::exception_ptr e);

    void deregister(ClientConnection& connection);

private:

    ClientConnectionRouter() {} ///< private constructor only used by singleton

    std::mutex connectionMutex_;
    // endpoint -> connection
    std::unordered_map<eckit::net::Endpoint, std::unique_ptr<ClientConnection>> connections_;
};

}