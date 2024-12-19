/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/s3/S3Common.h"

#include "eckit/config/LocalConfiguration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/s3/S3BucketName.h"
#include "eckit/io/s3/S3Config.h"
#include "eckit/io/s3/S3Session.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"

#include <algorithm>
#include <string>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

S3Common::S3Common(const fdb5::Config& config, const std::string& /*component*/, const fdb5::Key& key) {

    parseConfig(config);

    std::string keyStr = key.valuesToString();
    std::replace(keyStr.begin(), keyStr.end(), ':', '-');
    db_bucket_ = prefix_ + keyStr;
}

S3Common::S3Common(const fdb5::Config& config, const std::string& /*component*/, const eckit::URI& uri) {

    parseConfig(config);

    endpoint_ = eckit::net::Endpoint {uri.host(), uri.port()};

    db_bucket_ = eckit::S3BucketName::parse(uri.name());
}

void S3Common::parseConfig(const fdb5::Config& config) {

    eckit::LocalConfiguration s3 {};

    if (config.has("s3")) {
        s3 = config.getSubConfiguration("s3");

        std::string credentialsPath;
        if (s3.has("credential")) { credentialsPath = s3.getString("credential"); }
        eckit::S3Session::instance().loadCredentials(credentialsPath);
    }

    if (!s3.has("endpoint")) {
        throw eckit::UserError("Missing \"endpoint\" in configuration: " + config.configPath());
    }

    endpoint_ = eckit::net::Endpoint {s3.getString("endpoint", "127.0.0.1:9000")};

    eckit::S3Config s3Config(endpoint_);

    if (s3.has("region")) { s3Config.region = s3.getString("region"); }

    eckit::S3Session::instance().addClient(s3Config);

    prefix_ = s3.getString("bucketPrefix", prefix_);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
