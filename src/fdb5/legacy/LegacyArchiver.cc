/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/types/Metadata.h"


#include "fdb5/legacy/LegacyArchiver.h"
#include "fdb5/database/ArchiveVisitor.h"

namespace fdb5 {
namespace legacy {

//----------------------------------------------------------------------------------------------------------------------

LegacyArchiver::LegacyArchiver() :
    Archiver(),
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

    std::cout << "Metadata keys " << key << std::endl;
    std::cout << "Legacy keys " << legacy_ << std::endl;

    key.validateKeysOf(legacy_); //< compare legacy and metadata

    // archive

    ArchiveVisitor visitor(*this, key, blob->buffer(), blob->length());

    this->Archiver::archive(key, visitor);
}

void LegacyArchiver::legacy(const std::string &keyword, const std::string &value) {
    translator_.set(legacy_, keyword, value);
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace legacy
} // namespace fdb5
