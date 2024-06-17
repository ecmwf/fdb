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
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @author Simon Smart
/// @date   Dec 2016

#ifndef fdb5_PoolManager_H
#define fdb5_PoolManager_H

#include "eckit/utils/Regex.h"
#include "eckit/filesystem/PathName.h"

#include "fdb5/config/Config.h"
#include "fdb5/pmem/PoolGroup.h"

namespace metkit { class MarsRequest; }

namespace fdb5 {

class CanonicalKey;

//----------------------------------------------------------------------------------------------------------------------

class PoolManager  {

public: // methods

    PoolManager(const Config& config=Config());

    /// Uniquely selects a pool where the CanonicalKey will be put or already exists
    eckit::PathName pool(const CanonicalKey& key);

    /// Lists the roots that can be visited given a DB key
    std::vector<eckit::PathName> allPools(const CanonicalKey& key);

    /// Lists the roots that can be visited given a DB key
    std::vector<eckit::PathName> visitablePools(const CanonicalKey& key);
    std::vector<eckit::PathName> visitablePools(const std::set<CanonicalKey>& keys);
    std::vector<eckit::PathName> visitablePools(const metkit::mars::MarsRequest& request);

    /// Lists the roots where a DB key would be able to be written
    std::vector<eckit::PathName> writablePools(const CanonicalKey& key);

private: // members

    const std::vector<PoolGroup> poolGroupTable_;
    Config config_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
