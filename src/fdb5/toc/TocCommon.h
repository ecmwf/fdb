/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   TocCommon.h
/// @author Emanuele Danovaro
/// @date   August 2019

#pragma once

#include <dirent.h>

#include <string>
#include <vector>

#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/PathName.h"

#include "fdb5/api/helpers/ControlIterator.h"

namespace fdb5 {

class TocCommon {
public:

    TocCommon(const eckit::PathName& path);
    virtual ~TocCommon() = default;

    static eckit::LocalPathName findRealPath(const eckit::LocalPathName& path);
    static std::string userName(uid_t uid);

    /// @param path The base path to which the unique component will be appended.
    /// @return eckit::PathName::unique, with a 64-bit random token appended if the directory is on NFS.
    static eckit::PathName uniquePath(const eckit::PathName& path);

    virtual void checkUID() const;

    virtual const eckit::LocalPathName& basePath() const { return directory_; }

    std::string owner() const { return userName(dbUID()); }

protected:  // methods

    virtual uid_t dbUID() const;

protected:  // members

    const eckit::LocalPathName directory_;
    const eckit::LocalPathName schemaPath_;

    mutable uid_t dbUID_;
    uid_t userUID_;
};

//----------------------------------------------------------------------------------------------------------------------

class StdDir {

    eckit::PathName path_;
    DIR* d_;

public:

    StdDir(const eckit::PathName& p);
    ~StdDir();

    void children(std::vector<eckit::PathName>& paths);
};

}  // namespace fdb5
