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

#include "eckit/filesystem/PathName.h"
#include "eckit/io/FileHandle.h"
#include "eckit/thread/ThreadPool.h"
#include "eckit/serialisation/Streamable.h"

#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"
#include "fdb5/api/helpers/ControlIterator.h"

namespace fdb5 {

class TocCommon {
public:

    TocCommon(const eckit::PathName& path);
    virtual ~TocCommon() {}

    static eckit::PathName findRealPath(const eckit::PathName& path);
    static std::string userName(uid_t uid);

    virtual void checkUID() const;

    virtual const eckit::PathName& basePath() const { return directory_; }

    std::string owner() const { return userName(dbUID()); }

protected: // methods

    virtual uid_t dbUID() const;

protected: // members

    const eckit::PathName directory_;

    mutable uid_t dbUID_;
    uid_t userUID_;
};

}
