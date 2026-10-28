/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Metin Cakircali
/// @date Oct 2026

#pragma once

#include <memory>

#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/PathName.h"

namespace eckit {
class Configuration;
}

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// What a locked TOC file descriptor holds besides its file lock. Destroy it after the descriptor is closed.
class TocLock {
public:

    virtual ~TocLock() = default;
};

//----------------------------------------------------------------------------------------------------------------------

/// How the clients of a TOC are kept apart, which depends on the filesystem holding it.
class TocLocking {
public:

    virtual ~TocLocking() = default;

    /// The locking for the filesystem holding `directory` (for the whole process).
    /// @throws eckit::FailedSystemCall if the filesystem cannot be examined.
    static const TocLocking& forDirectory(const eckit::LocalPathName& directory);

    /// Checks that the locking can be relied on for `directory`, which may not exist yet. Call it before creating or
    /// opening a TOC there. `userConfig` may select what happens when it cannot.
    /// @throws eckit::UserError if it cannot, or if `userConfig` is invalid.
    virtual void validate(const eckit::LocalPathName& directory, const eckit::Configuration& userConfig) const = 0;

    /// Blocks until `fd` holds a shared lock. Returns what must outlive `fd`, or null if there is none.
    virtual std::unique_ptr<TocLock> holdForRead(int fd, const eckit::PathName& path) const = 0;

    /// Blocks until `fd` holds an exclusive lock. Returns what must outlive `fd`, or null if there is none.
    virtual std::unique_ptr<TocLock> holdForWrite(int fd, const eckit::PathName& path) const = 0;

    /// Whether the lock from holdForWrite() alone keeps other writers out, in this process and in others.
    virtual bool excludesWriters() const = 0;

    /// A name for a new file next to `path` that no other client generates.
    virtual eckit::PathName uniquePath(const eckit::PathName& path) const = 0;

protected:

    /// `path` if it exists, else its closest parent that exists.
    static eckit::LocalPathName locateExistingDirectory(const eckit::LocalPathName& path);
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
