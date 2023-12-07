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
#include "fdb5/remote/FdbEndpoint.h"

#include <unordered_map>

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

class Connection {
public:

    Connection(std::unique_ptr<ClientConnection> connection, Client& clients)
        : connection_(std::move(connection)), clients_(std::set<Client*>{&clients}) {}

    std::unique_ptr<ClientConnection> connection_;
    std::set<Client*> clients_;
};

//----------------------------------------------------------------------------------------------------------------------

class ClientConnectionRouter : eckit::NonCopyable {
public:

    static ClientConnectionRouter& instance();

    ClientConnection* connection(Client& client, const FdbEndpoint& endpoint);
    ClientConnection* connection(Client& client, const std::vector<FdbEndpoint>& endpoints);

    void deregister(Client& client);

private:

    ClientConnectionRouter() {} ///< private constructor only used by singleton

    std::mutex connectionMutex_;

    // endpoint -> connection
    std::unordered_map<FdbEndpoint, Connection> connections_;
};

}