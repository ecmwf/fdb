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

#include "eckit/types/Metadata.h"
#include "eckit/log/Log.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/ArchiveVisitor.h"

namespace fdb5 {
namespace legacy {

//----------------------------------------------------------------------------------------------------------------------

LegacyArchiver::LegacyArchiver(const eckit::Configuration& dbConfig) :
    fdb_(/* dbConfig */), // TODO: Pass dbConfig through. Currently clash between sink type/fdb type
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

//----------------------------------------------------------------------------------------------------------------------

} // namespace legacy
} // namespace fdb5
