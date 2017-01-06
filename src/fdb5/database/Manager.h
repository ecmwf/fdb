/*
 * (C) Copyright 1996-2016 ECMWF.
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

#include <string>

#include "eckit/filesystem/PathName.h"

namespace fdb5 {

class Key;

//----------------------------------------------------------------------------------------------------------------------

class Manager  {

public: // methods

    /// Uniquely selects the engine location that will handle this Key on insertion or if already exists
    static const std::string& engine(const Key &key);

    /// Uniquely selects a location where the Key will be put or already exists
    static eckit::PathName location(const Key &key);

    /// Lists the roots that can be visited given a DB key
    static std::vector<eckit::PathName> allLocations(const Key& key);

    /// Lists the roots that can be visited given a DB key
    static std::vector<eckit::PathName> visitableLocations(const Key& key);

    /// Lists the roots where a DB key would be able to be written
    static std::vector<eckit::PathName> writableLocations(const Key& key);

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
