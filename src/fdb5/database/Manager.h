// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @author Simon Smart
/// @date   Jan 2017

#pragma once

#include <set>
#include <string>
#include <vector>

#include "eckit/filesystem/URI.h"
#include "fdb5/config/Config.h"
#include "metkit/mars/MarsRequest.h"

namespace fdb5 {

class Key;

//----------------------------------------------------------------------------------------------------------------------

class Manager {

public:  // methods

    Manager(const Config& config);
    ~Manager();

    /// Uniquely selects the engine that will handle this Key on insertion or if already exists
    std::string engine(const Key& key);

    /// set union of all the engines that can possibly handle this key
    std::set<std::string> engines(const Key& key);
    std::set<std::string> engines(const metkit::mars::MarsRequest& rq, bool all);

    /// Uniquely selects the engine that will handle this URI by checking possible handlers
    std::string engine(const eckit::URI& uri);

    /// Lists the roots that can be visited given a DB key
    std::vector<eckit::URI> visitableLocations(const metkit::mars::MarsRequest& request, bool all);

private:  // members

    eckit::PathName enginesFile_;

    // If the engine is specified explicitly in the
    std::string explicitEngine_;

    Config config_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
