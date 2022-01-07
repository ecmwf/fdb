/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/config/Resource.h"
#include "fdb5/toc/TocSerialisationVersion.h"

namespace fdb5 {

static unsigned getUserEnvSerialisationVersion() {

    static unsigned fdbSerialisationVersion =
        eckit::Resource<unsigned>("fdbSerialisationVersion;$FDB5_SERIALISATION_VERSION", 0);
    
    if (fdbSerialisationVersion) {
        eckit::Log::debug() << "fdbSerialisationVersion overidde to version: " << fdbSerialisationVersion << std::endl;
    }
    return fdbSerialisationVersion; // default is 0 (not defined by user/service)
}

TocSerialisationVersion::TocSerialisationVersion(const fdb5::Config& config) {
    static unsigned envVersion = getUserEnvSerialisationVersion();
    if (envVersion) {
        bool valid = check(envVersion, false);
        if(not valid) {
            std::ostringstream msg;
            msg << "Unsupported FDB5 toc serialisation version " << envVersion
            << " - supported: " << supportedStr()
            << std::endl;
            throw eckit::BadValue(msg.str(), Here());
        }
        used_ = envVersion;
    } else {
        static int tocSerialisationVersion = config.getInt("tocSerialisationVersion", 0);
        if (tocSerialisationVersion && tocSerialisationVersion != defaulted()) {
            eckit::Log::debug() << "tocSerialisationVersion overidde to version: " << tocSerialisationVersion << std::endl;
            bool valid = check(tocSerialisationVersion, false);
            if(not valid) {
                std::ostringstream msg;
                msg << "Unsupported FDB5 toc serialisation version " << tocSerialisationVersion
                << " - supported: " << supportedStr()
                << std::endl;
                throw eckit::BadValue(msg.str(), Here());
            }
            used_ = tocSerialisationVersion;
        } else {
            used_ = defaulted();
        }
    }
}

TocSerialisationVersion::~TocSerialisationVersion() {}

std::vector<unsigned int> TocSerialisationVersion::supported() const {
    std::vector<unsigned int> versions = {3, 2, 1};
    return versions;
}

unsigned int TocSerialisationVersion::latest() const {
    return 3;
}

unsigned int TocSerialisationVersion::defaulted() const {
    return 2;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
