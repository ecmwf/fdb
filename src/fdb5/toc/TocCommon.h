// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   TocCommon.h
/// @author Emanuele Danovaro
/// @date   August 2019

#pragma once

#include <dirent.h>

#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/io/FileHandle.h"
#include "eckit/serialisation/Streamable.h"
#include "eckit/thread/ThreadPool.h"

#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"

namespace fdb5 {

class TocCommon {
public:

    TocCommon(const eckit::PathName& path);
    virtual ~TocCommon() = default;

    static eckit::LocalPathName findRealPath(const eckit::LocalPathName& path);
    static std::string userName(uid_t uid);

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
