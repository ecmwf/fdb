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

#include "eckit/filesystem/URI.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"

#include <algorithm>
#include <string>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

namespace {

/// @todo handle roots via fam root manager
auto parseRoot(const Config& config) -> eckit::URI {
    eckit::LocalConfiguration fam;

    if (config.has("fam_roots")) { fam = config.getSubConfigurations("fam_roots")[0]; }

    return eckit::URI {fam.getString("uri")};
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

auto FamCommon::toString(const Key& key) -> std::string {
    auto name = key.valuesToString();
    std::replace(name.begin(), name.end(), ':', '-');
    return name;
}

FamCommon::FamCommon(const eckit::FamRegionName& root): root_ {root} { }

FamCommon::FamCommon(const Config& config): FamCommon(parseRoot(config)) { }

/// @todo use key once fam root manager is implemented
FamCommon::FamCommon(const Config& config, const Key& /* key */): FamCommon(config) { }

//----------------------------------------------------------------------------------------------------------------------

auto FamCommon::exists() const -> bool {
    return root_.exists();
}

auto FamCommon::uri() const -> eckit::URI {
    return root_.uri();
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
