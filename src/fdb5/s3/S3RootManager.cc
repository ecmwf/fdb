/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the Horizon Europe programme funded project DaFab
 * (Grant agreement: 101128693) https://www.dafab-ai.eu/
 */

#include "fdb5/s3/S3RootManager.h"

#include "eckit/config/LocalConfiguration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/io/s3/S3BucketName.h"
#include "eckit/io/s3/S3Config.h"
#include "eckit/io/s3/S3Session.h"
#include "eckit/log/CodeLocation.h"
#include "eckit/log/Log.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/config/Config.h"
#include "fdb5/s3/S3Root.h"

#include <ostream>
#include <string>
#include <vector>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

namespace {

auto loadS3(const Config& config) -> eckit::LocalConfiguration {

    eckit::Log::debug<LibFdb5>() << "Loading S3 configuration from: " << config.configPath() << std::endl;

    const auto s3Config = config.getSubConfiguration("s3");

    if (s3Config.empty()) { throw eckit::UserError("No S3 configuration found in: " + config.configPath()); }

    {
        auto& session = eckit::S3Session::instance();

        // credentials

        if (s3Config.has("credentials")) {
            const auto credentialsPath = s3Config.getString("credentials");
            session.loadCredentials(credentialsPath);
        }

        // configuration clients

        std::string configPath;
        if (s3Config.has("configuration")) { configPath = s3Config.getString("configuration"); }
        session.loadClients(configPath);

        //         const auto roots = s3Config.getSubConfigurations("roots");
        //         eckit::Log::info() << "----------> serversPath: " << serversPath << std::endl;
        //         session.loadClients(serversPath);
    }

    return s3Config;
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

S3RootManager::S3RootManager(const Config& config) : config_ {loadS3(config)} { }

//----------------------------------------------------------------------------------------------------------------------

eckit::S3BucketName S3RootManager::root(const Key& /*databaseKey*/) const {
    /// @todo implement databaseKey to root selection
    return parseRoots().front();
}

std::vector<eckit::S3BucketName> S3RootManager::parseRoots() const {

    const auto roots = config_.getSubConfigurations("roots");

    if (roots.empty()) { throw eckit::UserError("No S3 roots found in configuration!", Here()); }

    std::vector<eckit::S3BucketName> result;
    result.reserve(roots.size());

    for (const auto& root : roots) { result.emplace_back(root.getString("endpoint"), root.getString("bucket")); }

    return result;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
