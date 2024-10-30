/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the Horizon Europe programme funded project OpenCUBE
 * (Grant agreement: 101092984) horizon-opencube.eu
 */

#include "fdb5/fam/FamRecordVersion.h"

#include "eckit/config/Resource.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/config/Config.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

namespace {

using value_type = FamRecordVersion::value_type;

auto getEnvRecordVersion() -> value_type {
    static const value_type version = eckit::Resource<value_type>("fdbFamRecordVersion;$FDB_FAM_RECORD_VERSION", 0);

    if (version > 0 && version != FamRecordVersion::defaulted) {
        LOG_DEBUG_LIB(LibFdb5) << "Non-default FAM record version: " << version << '\n';
    }

    return version;
}

auto parseVersion(const Config& config) -> value_type {
    auto version = getEnvRecordVersion();
    if (version == 0) {
        version = config.getUnsigned("FamRecordVersion", FamRecordVersion::defaulted);
        if (version != FamRecordVersion::defaulted) {
            LOG_DEBUG_LIB(LibFdb5) << "Using non-default FAM record version: " << version << '\n';
        }
    }
    return version;
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

void FamRecordVersion::check(const value_type version) {
    if (std::find(supported.begin(), supported.end(), version) == supported.end()) {
        std::ostringstream msg;
        msg << "FDB5 FAM record version (" << version << ") is not supported! " << supportedStr() << '\n';
        throw eckit::BadValue(msg.str(), Here());
    }
}

auto FamRecordVersion::supportedStr() -> std::string {
    std::string result;

    if (result.empty()) {
        using namespace std::string_literals;

        result = "Supported record " + (supported.size() > 1 ? "versions: "s : "version: "s);

        for (auto&& version : supported) {
            result += std::to_string(version);
            if (version != supported.back()) { result += ','; }
        }
    }

    return result;
}

//----------------------------------------------------------------------------------------------------------------------

FamRecordVersion::FamRecordVersion(const Config& config): value_ {parseVersion(config)} {
    check(value_);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
