/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/remote/RemoteEngine.h"

// #include "eckit/io/Buffer.h"
// #include "eckit/serialisation/MemoryStream.h"

#include "fdb5/config/Config.h"
// #include "fdb5/remote/client/Client.h"


namespace fdb5::remote {

// class RemoteEngineClient : public Client {

// public:

//     RemoteEngineClient(const Config& config) :
//         Client(eckit::net::Endpoint(config.getString("host"), config.getInt("port"))) {
//     }

// private: // methods

//     // Client
//     bool handle(remote::Message message, uint32_t requestID) override { return false; }
//     bool handle(remote::Message message, uint32_t requestID, eckit::net::Endpoint endpoint, eckit::Buffer&& payload) override { return false; }
//     void handleException(std::exception_ptr e) override { NOTIMP; }

//     const Key& key() const override { NOTIMP; }

// private: // members

// };

//----------------------------------------------------------------------------------------------------------------------

std::string RemoteEngine::name() const {
    return RemoteEngine::typeName();
}

std::string RemoteEngine::dbType() const {
    return RemoteEngine::typeName();
}

bool RemoteEngine::canHandle(const eckit::URI& uri) const
{
    return uri.scheme() == "fdb";
}

std::vector<eckit::URI> RemoteEngine::visitableLocations(const Config& config) const
{
    // if (!client_) {
    //     client_.reset(new RemoteEngineClient(config));
    // }
    // ASSERT(client_);

    return std::vector<eckit::URI> {}; //databases(Key(), CatalogueRootManager(config).visitableRoots(InspectionKey()), config);
}

std::vector<eckit::URI> RemoteEngine::visitableLocations(const metkit::mars::MarsRequest& request, const Config& config) const
{
    // if (!client_) {
    //     client_.reset(new RemoteEngineClient(config));
    // }
    // ASSERT(client_);

    return std::vector<eckit::URI> {}; //databases(request, CatalogueRootManager(config).visitableRoots(request), config);
}

void RemoteEngine::print(std::ostream& out) const
{
    out << "RemoteEngine()";
}

static EngineBuilder<RemoteEngine> remote_builder;

//static eckit::LocalFileManager manager_toc("toc");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5::remote
