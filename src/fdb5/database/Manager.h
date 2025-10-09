/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @author Simon Smart
/// @date   Jan 2017

#ifndef fdb5_Manager_H
#define fdb5_Manager_H

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

#endif
