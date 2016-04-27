/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Timer.h"
#include "eckit/log/Plural.h"
#include "eckit/log/Bytes.h"
#include "eckit/log/Seconds.h"
#include "eckit/log/Progress.h"
#include "eckit/types/Metadata.h"

#include "marslib/EmosFile.h"

#include "fdb5/legacy/LegacyArchiver.h"
#include "fdb5/ArchiveVisitor.h"

namespace fdb5 {
namespace legacy {

//----------------------------------------------------------------------------------------------------------------------

LegacyArchiver::LegacyArchiver() :
    Archiver(),
    translator_(),
    legacy_()
{
}

void LegacyArchiver::archive(const eckit::DataBlobPtr blob)
{
    const eckit::Metadata& metadata = blob->metadata();

    Key key;

    eckit::StringList keywords = metadata.keywords();

    std::string value;
    for(eckit::StringList::const_iterator i = keywords.begin(); i != keywords.end(); ++i) {
        metadata.get(*i, value);
        key.set(*i, value);
    }

    // compare legacy and metadata

    eckit::StringSet missing;
    eckit::StringSet mismatch;

    const eckit::StringDict& f = key.dict();
    const eckit::StringDict& k = legacy_.dict();

    for(eckit::StringDict::const_iterator j = f.begin(); j != f.end(); ++j) {

        eckit::StringDict::const_iterator itr = k.find((*j).first);
        if(itr == k.end()) {
            missing.insert((*j).first);
        }
        else {
            if(j->second != itr->second) {
                std::ostringstream oss;
                oss << j->first << "=" << j->second << " and " << itr->second;
                mismatch.insert(oss.str());
            }
        }
    }

    if(missing.size() || mismatch.size()) {

        std::ostringstream oss;
        oss << "FDB LegacyArchiver missing keys: " << missing;
        oss << " mismatch keys: " << mismatch;

        throw eckit::SeriousBug(oss.str());
    }

    // archive

    ArchiveVisitor visitor(*this, key, blob->buffer(), blob->length());

    this->Archiver::archive(key, visitor);
}

void LegacyArchiver::legacy(const std::string& keyword, const std::string& value)
{
    translator_.set(legacy_, keyword, value);
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace legacy
} // namespace fdb5
