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

}  // namespace fdb5
