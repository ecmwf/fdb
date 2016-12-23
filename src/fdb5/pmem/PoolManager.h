/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   PoolManager.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @author Simon Smart
/// @date   Dec 2016

#ifndef fdb5_PoolManager_H
#define fdb5_PoolManager_H

#include "eckit/utils/Regex.h"
#include "eckit/filesystem/PathName.h"

namespace fdb5 {

class Key;

//----------------------------------------------------------------------------------------------------------------------

class PoolManager  {

public: // methods

    /// Uniquely selects a pool where the Key will be put or already exists
    static eckit::PathName pool(const Key &key);

    /// Lists the roots that can be visited given a DB key
    static std::vector<eckit::PathName> allPools(const Key& key);

    /// Lists the roots that can be visited given a DB key
    static std::vector<eckit::PathName> visitablePools(const Key& key);

    /// Lists the roots where a DB key would be able to be written
    static std::vector<eckit::PathName> writablePools(const Key& key);
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
