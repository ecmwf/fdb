/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <algorithm>

#include "eckit/io/s3/S3Name.h"
#include "eckit/io/s3/S3Credential.h"
#include "eckit/io/s3/S3Session.h"

#include "fdb5/s3/S3Common.h"

// #include "eckit/exception/Exceptions.h"
#include "eckit/config/Resource.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

S3Common::S3Common(const fdb5::Config& config, const std::string& component, const fdb5::Key& key) {

    parseConfig(config);



    /// @note: code for bucket per DB

    std::string keyStr = key.valuesToString();
    std::replace(keyStr.begin(), keyStr.end(), ':', '-');
    db_bucket_ = prefix_ + keyStr;




    /// @note: code for single bucket for all DBs

    // std::vector<std::string> valid{"catalogue", "store"};
    // ASSERT(std::find(valid.begin(), valid.end(), component) != valid.end());

    // bucket_ = "default";

    // eckit::LocalConfiguration c{};

    // if (config.has("s3")) c = config.getSubConfiguration("s3");
    // if (c.has(component)) bucket_ = c.getSubConfiguration(component).getString("bucket", bucket_);

    // std::string first_cap{component};
    // first_cap[0] = toupper(component[0]);

    // std::string all_caps{component};
    // for (auto & c: all_caps) c = toupper(c);

    // bucket_ = eckit::Resource<std::string>("fdbS3" + first_cap + "Bucket;$FDB_S3_" + all_caps + "_BUCKET", bucket_);

    // db_prefix_ = key.valuesToString();

    // if (c.has("client"))
    //     fdb5::DaosManager::instance().configure(c.getSubConfiguration("client"));




    /// @todo: check that the bucket name complies with name restrictions

}

S3Common::S3Common(const fdb5::Config& config, const std::string& component, const eckit::URI& uri) {

    /// @note: validity of input URI is not checked here because this constructor is only triggered
    ///   by DB::buildReader in EntryVisitMechanism, where validity of URIs is ensured beforehand

    parseConfig(config);

    endpoint_ = eckit::net::Endpoint{uri.host(), uri.port()};



    /// @note: code for bucket per DB

    const auto parts = eckit::Tokenizer("/").tokenize(uri.name());
    const auto n = parts.size();
    ASSERT(n == 1 | n == 2);
    db_bucket_ = parts[0];



    /// @note: code for single bucket for all DBs

    // eckit::S3Name n{uri};

    // bucket_ = n.bucket().name();

    // eckit::Tokenizer parse("_");
    // std::vector<std::string> bits;
    // parse(n.name(), bits);

    // ASSERT(bits.size() == 2);

    // db_prefix_ = bits[0];


    // // eckit::LocalConfiguration c{};

    // // if (config.has("s3")) c = config.getSubConfiguration("s3");

    // // if (c.has("client"))
    // //     fdb5::DaosManager::instance().configure(c.getSubConfiguration("client"));

}

void S3Common::parseConfig(const fdb5::Config& config) {

    eckit::LocalConfiguration s3{};

    if (config.has("s3")) {
        s3 = config.getSubConfiguration("s3");
        
        std::string credentialsPath;
        if (s3.has("credential")) { credentialsPath = s3.getString("credential"); }
        eckit::S3Session::instance().readCredentials(credentialsPath);
    }
    

    endpoint_ = eckit::net::Endpoint{s3.getString("endpoint", "127.0.0.1:9000")};



    /// @note: code for bucket per DB only
    prefix_ = s3.getString("bucketPrefix", prefix_);

}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5