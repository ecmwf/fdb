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

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/PathName.h"

#include "fdb5/toc/detail/TocLocking.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// Locking for TOCs on NFS: blocking fcntl locks served by the NFS server, a mutex per file to keep the handlers of
/// this process apart, and file names that another client cannot generate.
class NFSTocLocking final : public TocLocking {
public:  // types

    /// What to do when an NFS mount has client-local locks, which do not keep other clients out.
    enum class Policy {
        Warn,   ///< log a warning, once per filesystem
        Error,  ///< throw eckit::UserError
        Ignore  ///< do nothing
    };

    /// The mount option that makes the locks of a device ("major:minor") client-local, if there is one.
    using Lookup = std::function<std::optional<std::string>(const std::string& device)>;

    /// Parses "warn", "error" or "ignore" (case-insensitive).
    /// @throws eckit::UserError for any other value.
    static Policy parsePolicy(const std::string& value);

public:  // methods

    NFSTocLocking(Policy policy, Lookup lookup);

    /// Takes the policy from $FDB_NFS_LOCAL_LOCK_POLICY (default "warn") and reads the mount options from the system.
    /// Only Linux reports them: elsewhere no mount is client-local as far as this class can tell.
    /// @throws eckit::UserError if the variable holds an unknown policy.
    NFSTocLocking();

    /// Applies the policy if the mount holding `directory` has client-local locks. A filesystem is looked up once.
    /// Warn reports it once, Error throws on every call.
    /// @throws eckit::UserError under Policy::Error.
    void validate(const eckit::LocalPathName& directory) const override;

    std::unique_ptr<TocLock> holdForRead(int fd, const eckit::PathName& path) const override;

    std::unique_ptr<TocLock> holdForWrite(int fd, const eckit::PathName& path) const override;

    bool excludesWriters() const override { return true; }

    eckit::PathName uniquePath(const eckit::PathName& path) const override;

private:  // members

    Policy policy_;
    Lookup lookup_;

    mutable std::mutex mutex_;
    mutable std::map<std::string, std::optional<std::string>> options_;  ///< by device
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
