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
/// @date   Mar 2016

#ifndef fdb5_RootManager_H
#define fdb5_RootManager_H

#include "eckit/config/LocalConfiguration.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/utils/Regex.h"

#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/config/Config.h"
#include "fdb5/toc/FileSpace.h"

namespace metkit {
namespace mars {
class MarsRequest;
}
}  // namespace metkit

namespace fdb5 {

class Key;
class FileSpace;
class DbPathNamer;

//----------------------------------------------------------------------------------------------------------------------

typedef std::vector<fdb5::FileSpace> FileSpaceTable;

class RootManager {

public:  // methods

    RootManager(const Config& config);

    /// Uniquely selects a directory where the Key will be put or already exists
    TocPath directory(const Key& key);

    /// Lists the roots that can be visited given a DB key
    // std::vector<eckit::PathName> allRoots(const Key& key);

    /// Lists the roots that can be visited given a DB key
    std::vector<eckit::PathName> visitableRoots(const Key& key);
    std::vector<eckit::PathName> visitableRoots(const std::set<Key>& keys);
    std::vector<eckit::PathName> visitableRoots(const metkit::mars::MarsRequest& request);

    /// Lists the roots where a DB key would be able to be written
    std::vector<eckit::PathName> canArchiveRoots(const Key& key);
    std::vector<eckit::PathName> canMoveToRoots(const Key& key);

    std::string dbPathName(const Key& key);

    std::vector<std::string> possibleDbPathNames(const Key& key, const char* missing);

protected:  // methods

    virtual FileSpaceTable fileSpaces();
    virtual std::vector<eckit::LocalConfiguration> getSpaceRoots(const eckit::LocalConfiguration& space) = 0;

protected:  // members

    std::vector<FileSpace> spacesTable_;

private:  // members

    const std::vector<DbPathNamer>& dbPathNamers_;
    Config config_;
};

class CatalogueRootManager : public RootManager {

public:  // methods

    CatalogueRootManager(const Config& config) : RootManager(config) { spacesTable_ = fileSpaces(); }

protected:  // methods

    std::vector<eckit::LocalConfiguration> getSpaceRoots(const eckit::LocalConfiguration& space) override;
};

class StoreRootManager : public RootManager {

public:  // methods

    StoreRootManager(const Config& config) : RootManager(config) { spacesTable_ = fileSpaces(); }

protected:  // methods

    std::vector<eckit::LocalConfiguration> getSpaceRoots(const eckit::LocalConfiguration& space) override;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif
