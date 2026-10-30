// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "eckit/config/Resource.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/toc/TocSerialisationVersion.h"

namespace fdb5 {

static unsigned getUserEnvSerialisationVersion() {

    static unsigned fdbSerialisationVersion =
        eckit::Resource<unsigned>("fdbSerialisationVersion;$FDB_SERIALISATION_VERSION", 0);
    if (!fdbSerialisationVersion) {
        // backwards compatibility
        fdbSerialisationVersion = eckit::Resource<unsigned>("$FDB5_SERIALISATION_VERSION", 0);
    }
    if (fdbSerialisationVersion && fdbSerialisationVersion != TocSerialisationVersion::defaulted()) {
        LOG_DEBUG_LIB(LibFdb5) << "fdbSerialisationVersion overidde to version: " << fdbSerialisationVersion
                               << std::endl;
    }
    return fdbSerialisationVersion;  // default is 0 (not defined by user/service)
}

TocSerialisationVersion::TocSerialisationVersion(const fdb5::Config& config) {
    static unsigned envVersion = getUserEnvSerialisationVersion();
    if (envVersion) {
        used_ = envVersion;
    }
    else {
        static int tocSerialisationVersion = config.getInt("tocSerialisationVersion", 0);
        if (tocSerialisationVersion && tocSerialisationVersion != TocSerialisationVersion::defaulted()) {
            LOG_DEBUG_LIB(LibFdb5) << "tocSerialisationVersion overidde to version: " << tocSerialisationVersion
                                   << std::endl;
            used_ = tocSerialisationVersion;
        }
        else {
            used_ = defaulted();
        }
    }

    bool valid = check(used_, false);
    if (not valid) {
        std::ostringstream msg;
        msg << "Unsupported FDB5 toc serialisation version " << envVersion << " - supported: " << supportedStr()
            << std::endl;
        throw eckit::BadValue(msg.str(), Here());
    }
}

TocSerialisationVersion::~TocSerialisationVersion() {}

std::vector<unsigned int> TocSerialisationVersion::supported() {
    std::vector<unsigned int> versions = {3, 2, 1};
    return versions;
}

unsigned int TocSerialisationVersion::latest() {
    return 3;
}

unsigned int TocSerialisationVersion::defaulted() {
    return 3;
}

unsigned int TocSerialisationVersion::used() const {
    return used_;
}

std::string TocSerialisationVersion::supportedStr() {
    std::ostringstream oss;
    char sep = '[';
    for (auto v : supported()) {
        oss << sep << v;
        sep = ',';
    }
    oss << ']';
    return oss.str();
}

bool TocSerialisationVersion::check(unsigned int version, bool throwOnFail) const {
    std::vector<unsigned int> versionsSupported = supported();
    for (auto v : versionsSupported) {
        if (version == v) {
            return true;
        }
    }
    if (throwOnFail) {
        std::ostringstream msg;
        msg << "Record version mismatch, software supports versions " << supportedStr() << " got " << version;
        throw eckit::SeriousBug(msg.str());
    }
    return false;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
