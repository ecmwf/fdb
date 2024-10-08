/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/toc/TocSerialisationVersion.h"
#include "eckit/log/Log.h"
#include "fdb5/LibFdb5.h"

#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"

#include <array>
#include <string>
#include <vector>

namespace fdb5 {

namespace {

constexpr std::array<TocSerialisationVersion::version_type, 4> supportedVersions = {4, 3, 2, 1};

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

TocSerialisationVersion::TocSerialisationVersion(const fdb5::Config& config) {
    static version_type fdbSerialisationVersion =
        eckit::Resource<version_type>("fdbSerialisationVersion;$FDB5_SERIALISATION_VERSION", 0);

    LOG_DEBUG_LIB(LibFdb5) << "FDB serialisation version: " << fdbSerialisationVersion << std::endl;

    // version=0 means undefined
    if (fdbSerialisationVersion > 0) {
        used_ = fdbSerialisationVersion;
    } else {
        static version_type tocSerialisationVersion = config.getInt("tocSerialisationVersion", 0);

        LOG_DEBUG_LIB(LibFdb5) << "TOC Serialisation Version: " << tocSerialisationVersion << std::endl;

        if (tocSerialisationVersion != 0 && tocSerialisationVersion != TocSerialisationVersion::defaulted()) {
            used_ = tocSerialisationVersion;
        } else {
            used_ = defaulted();
        }
    }

    LOG_DEBUG_LIB(LibFdb5) << "Using TOC serialisation version: " << used_ << std::endl;

    if (!check(used_, false)) {
        std::ostringstream msg;
        msg << "TOC serialisation version [" << used_ << "] is NOT valid! Supported: " << supportedStr() << std::endl;
        throw eckit::BadValue(msg.str(), Here());
    }
}

auto TocSerialisationVersion::supported() -> std::vector<version_type> {
    return {supportedVersions.begin(), supportedVersions.end()};
}

auto TocSerialisationVersion::latest() -> version_type {
    return supportedVersions.front();
}

auto TocSerialisationVersion::defaulted() -> version_type {
    return 2;
}

auto TocSerialisationVersion::used() const -> version_type {
    return used_;
}

std::string TocSerialisationVersion::supportedStr() {
    std::ostringstream oss;

    char sep = '[';
    for (const auto& version : supportedVersions) {
        oss << sep << version;
        sep = ',';
    }
    oss << ']';

    return oss.str();
}

bool TocSerialisationVersion::check(const version_type version, const bool throwOnFail) {

    for (const auto& supported : supportedVersions) {
        if (version == supported) { return true; }
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
