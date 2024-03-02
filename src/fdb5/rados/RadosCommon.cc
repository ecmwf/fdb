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

#include "eckit/io/rados/RadosObject.h"
// #include "eckit/io/s3/S3Bucket.h"
// #include "eckit/io/s3/S3Credential.h"
// #include "eckit/io/s3/S3Session.h"

#include "fdb5/rados/RadosCommon.h"

#include "eckit/exception/Exceptions.h"
// #include "eckit/config/Resource.h"
#include "eckit/utils/Tokenizer.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

RadosCommon::RadosCommon(const fdb5::Config& config, const std::string& component, const fdb5::Key& key) {

    std::vector<std::string> valid{"catalogue", "store"};
    ASSERT(std::find(valid.begin(), valid.end(), component) != valid.end());

    parseConfig(config, component);

    eckit::LocalConfiguration rados{}, comp_conf{};

    if (config.has("rados")) {
        rados = config.getSubConfiguration("rados");
        if (rados.has(component)) comp_conf = rados.getSubConfiguration(component);
    }

#ifdef fdb5_HAVE_RADOS_STORE_SINGLE_POOL

    pool_ = rados.getString("pool", pool_);
    pool_ = comp_conf.getString("pool", pool_);

    // std::string first_cap{component};
    // first_cap[0] = toupper(component[0]);
    // std::string all_caps{component};
    // for (auto & c: all_caps) c = toupper(c);
    // bucket_ = eckit::Resource<std::string>("fdbRados" + first_cap + "Pool;$FDB_RADOS_" + all_caps + "_POOL", pool_);

    ASSERT_MSG(pool_.length() > 0, "No pool configured for Rados " + component);

    db_namespace_ = key.valuesToString();

#else

    prefix_ = rados.getString("poolPrefix", prefix_);
    prefix_ = comp_conf.getString("poolPrefix", prefix_);
    ASSERT_MSG(prefix_.find("_") == std::string::npos, "The configured poolPrefix must not contain underscores.");
    db_pool_ = prefix_ + "_" + key.valuesToString();
    namespace_ = "default";

#endif

}

RadosCommon::RadosCommon(const fdb5::Config& config, const std::string& component, const eckit::URI& uri) {

    /// @note: validity of input URI is not checked here because this constructor is only triggered
    ///   by DB::buildReader in EntryVisitMechanism, where validity of URIs is ensured beforehand

    parseConfig(config, component);

#ifdef fdb5_HAVE_RADOS_STORE_SINGLE_POOL

    eckit::RadosObject o{uri};
    pool_ = o.nspace().pool().name();
    db_namespace_ = o.nspace().name();

#else

    eckit::RadosObject o{uri};
    db_pool_ = o.nspace().pool().name();
    namespace_ = o.nspace().name();
    if (namespace_ != "default")
        throw eckit::SeriousBug("Unexpected namespace name '" + namespace_ + "'. Expected 'default'.");
    const auto parts = eckit::Tokenizer("_").tokenize(db_pool_);
    const auto n = parts.size();
    ASSERT(n > 1);
    prefix_ = parts[0];

#endif

}

void RadosCommon::parseConfig(const fdb5::Config& config, const std::string& component) {

    eckit::LocalConfiguration rados{}, comp_conf{};

    if (config.has("rados")) {
        rados = config.getSubConfiguration("rados");
        if (rados.has(component)) comp_conf = rados.getSubConfiguration(component);
    }

    maxObjectSize_ = rados.getInt("maxObjectSize", 0);

}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5