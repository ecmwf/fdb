/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

// #include <algorithm>

#include "eckit/s3/S3Name.h"

#include "fdb5/s3/S3Common.h"

// #include "eckit/exception/Exceptions.h"
#include "eckit/config/Resource.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

S3Common::S3Common(const fdb5::Config& config, const std::string& component, const fdb5::Key& key) {

    /// @note: code for bucket per DB

    db_bucket_ = key.valuesToString();


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

}

S3Common::S3Common(const fdb5::Config& config, const std::string& component, const eckit::URI& uri) {

    /// @note: validity of input URI is not checked here because this constructor is only triggered
    ///   by DB::buildReader in EntryVisitMechanism, where validity of URIs is ensured beforehand
    

    /// @note: code for bucket per DB

    db_bucket_ = eckit::S3Name{uri}.bucketName();



    /// @note: code for single bucket for all DBs

    // eckit::S3Name n{uri};

    // bucket_ = n.bucketName();

    // eckit::Tokenizer parse("_");
    // std::vector<std::string> bits;
    // parse(n.keyName(), bits);

    // ASSERT(bits.size() == 2);

    // db_prefix_ = bits[0];


    // // eckit::LocalConfiguration c{};

    // // if (config.has("s3")) c = config.getSubConfiguration("s3");

    // // if (c.has("client"))
    // //     fdb5::DaosManager::instance().configure(c.getSubConfiguration("client"));

}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5