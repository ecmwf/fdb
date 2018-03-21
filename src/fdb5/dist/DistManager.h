/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @date   Mar 2018

#ifndef fdb5_dist_DistManager_H
#define fdb5_dist_DistManager_H

#include "eckit/utils/Regex.h"
#include "eckit/filesystem/PathName.h"

#include <vector>

namespace fdb5 {

class FDBConfig;
class Key;
class FDBDistPool;

//----------------------------------------------------------------------------------------------------------------------

/// Keep track of the available pools/lanes given different fdb_home variables.

class DistManager  {

public: // methods

    DistManager(const FDBConfig& config);
    ~DistManager();

    void writableLanes(const Key& key, std::vector<eckit::PathName>& lanes) const;

private: // members

    const std::vector<FDBDistPool>& poolTable_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif // fb5_dist_DistManager_H
