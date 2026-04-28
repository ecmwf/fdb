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

#include "fdb5/fam/FamCommon.h"

#include <algorithm>
#include <climits>
#include <string>
#include <utility>

#include "eckit/filesystem/URI.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/io/fam/FamRegionName.h"
#include "eckit/serialisation/HandleStream.h"
#include "eckit/serialisation/MemoryStream.h"

#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"
#include "fdb5/fam/FamEngine.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

std::string FamCommon::toString(const Key& key) {
    auto name = key.valuesToString();
    std::replace(name.begin(), name.end(), ':', '-');
    return name;
}

std::string FamCommon::encodeKey(const Key& key) {
    eckit::MemoryHandle h{static_cast<size_t>(PATH_MAX)};
    eckit::HandleStream hs{h};
    h.openForWrite(0);
    {
        eckit::AutoClose c(h);
        hs << key;
    }
    return {static_cast<const char*>(h.data()), static_cast<std::size_t>(hs.bytesWritten())};
}

Key FamCommon::decodeKey(eckit::MemoryStream key) {
    return Key{key};
}

FamCommon::FamCommon(eckit::FamRegionName root) : root_{std::move(root)} {}

FamCommon::FamCommon(const eckit::URI& root) : FamCommon(eckit::FamRegionName(root)) {}

FamCommon::FamCommon(const Config& config) : FamCommon(FamEngine::rootURI(config)) {}

/// @todo use key once fam root manager is implemented
FamCommon::FamCommon(const Key& /*key*/, const Config& config) : FamCommon(config) {}

/// @todo use uri once fam root manager is implemented
FamCommon::FamCommon(const eckit::URI& /*uri*/, const Config& config) : FamCommon(config) {}

//----------------------------------------------------------------------------------------------------------------------

bool FamCommon::exists() const {
    return root_.exists();
}

eckit::URI FamCommon::uri() const {
    return root_.uri();
}

const eckit::FamRegion& FamCommon::getRegion() const {
    std::call_once(region_once_, [this] { region_.emplace(root_.lookup()); });
    return *region_;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
