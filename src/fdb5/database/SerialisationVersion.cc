/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <cstring>
#include <vector>

#include "eckit/exception/Exceptions.h"
#include "fdb5/database/SerialisationVersion.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

SerialisationVersion::SerialisationVersion() {
    used_ = 0;
}

unsigned int SerialisationVersion::used() const {
    return used_;
}

std::string SerialisationVersion::supportedStr() const {
    std::ostringstream oss;
    char sep = '[';
    for (auto v : supported()) {
        oss << sep << v;
        sep = ',';
    }
    oss << ']';
    return oss.str();
}

bool SerialisationVersion::check(unsigned int version, bool throwOnFail) const {
    std::vector<unsigned int> versionsSupported = supported();
    for (auto v : versionsSupported) {
        if (version == v)
            return true;
    }
    if (throwOnFail) {
        std::ostringstream msg;
        msg << "Record version mismatch, software supports versions " << supportedStr() << " got " << version;
        throw eckit::SeriousBug(msg.str());
    }
    return false;
}

}  // namespace fdb5
