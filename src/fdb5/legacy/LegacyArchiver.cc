/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/legacy/LegacyArchiver.h"

#include "eckit/config/LocalConfiguration.h"
#include "eckit/log/Log.h"
#include "eckit/types/Metadata.h"
#include "eckit/value/Value.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/ArchiveVisitor.h"

namespace fdb5 {
namespace legacy {

//----------------------------------------------------------------------------------------------------------------------

// FDB-130
// Multio creates a default configuration of {"type": "fdb5", "useSubToc": true}
//
// This is an unfortunate mixing of responsibilities, whereby the type of sink
// being used (type=fdb5) becomes mixed with the fdb configuration. What we want
// to do is ensure that if this is happening, we strip out the "type=fdb5" such
// that the default configuration continues to be used (supplemented by other
// supplied configuration).


/// Eckit::LocalConfiguration does not allow us to remove values!!!
class TweakableConfiguration : public eckit::LocalConfiguration {
public:
    using LocalConfiguration::LocalConfiguration;

    void unset(const std::string& name) {
        ASSERT(has(name));
        ASSERT(root_);
        if (root_->shared()) {
            (*root_) = root_->clone();
        }
        ASSERT(!root_->shared());
        ASSERT(root_->contains(name));
        root_->remove(name);
    }
};


eckit::LocalConfiguration tweakedConfig(const eckit::Configuration& dbConfig) {

    if (dbConfig.has("type")) {
        if (dbConfig.getString("type") == "fdb5") {
            TweakableConfiguration cfg(dbConfig);
            ASSERT(cfg.has("type"));
            ASSERT(cfg.getString("type") == "fdb5");
            cfg.unset("type");
            return cfg;
        }
    }
    return eckit::LocalConfiguration(dbConfig);
}

//----------------------------------------------------------------------------------------------------------------------

LegacyArchiver::LegacyArchiver(const eckit::Configuration& dbConfig) :
    fdb_(tweakedConfig(dbConfig)),
    translator_(),
    legacy_() {
}

void LegacyArchiver::archive(const eckit::DataBlobPtr blob) {
    const eckit::Metadata &metadata = blob->metadata();

    Key key;

    eckit::StringList keywords = metadata.keywords();

    std::string value;
    for (eckit::StringList::const_iterator i = keywords.begin(); i != keywords.end(); ++i) {
        metadata.get(*i, value);
        key.set(*i, value);
    }

    eckit::Log::debug<LibFdb5>() << "Metadata keys " << key << std::endl;
    eckit::Log::debug<LibFdb5>() << "Legacy keys " << legacy_ << std::endl;

    key.validateKeysOf(legacy_); //< compare legacy and metadata

    // archive

    fdb_.archive(key, blob->buffer(), blob->length());
}

void LegacyArchiver::flush() {
    fdb_.flush();
}

void LegacyArchiver::legacy(const std::string &keyword, const std::string &value) {
    translator_.set(legacy_, keyword, value);
}

const FDB& LegacyArchiver::fdb() const {
    return fdb_;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace legacy
} // namespace fdb5
