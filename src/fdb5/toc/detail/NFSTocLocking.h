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
    enum class LocalLockPolicy {
        Error,  ///< throw eckit::UserError
        Allow   ///< go ahead, logging a warning once per filesystem
    };

    /// Finds the mount option that makes the locks of a device ("major:minor") client-local, if there is one.
    using LocalLockLookup = std::function<std::optional<std::string>(const std::string& device)>;

    /// Parses the name of a policy (case-insensitive). `source` names where `value` came from, for the error.
    /// @throws eckit::UserError for any other value.
    static LocalLockPolicy parsePolicy(const std::string& value, const std::string& source = "NFS local lock policy");

    /// The policy from $FDB_NFS_LOCAL_LOCK_POLICY if set, else from "nfsLocalLockPolicy" in `config` (the config of
    /// the FDB, not the user config), else LocalLockPolicy::Error.
    /// @throws eckit::UserError if the value is not a policy.
    static LocalLockPolicy resolvePolicy(const eckit::Configuration& config);

public:  // methods

    /// `lookup` stands in for the mount options of the system.
    explicit NFSTocLocking(LocalLockLookup lookup);

    /// Reads the mount options from the system.
    /// Only Linux reports them: elsewhere no mount is client-local as far as this class can tell.
    NFSTocLocking();

    /// Applies the policy resolved from `config` (see resolvePolicy()).
    /// @throws eckit::UserError if the policy is invalid, or under LocalLockPolicy::Error.
    void validate(const eckit::LocalPathName& directory, const eckit::Configuration& config) const override;

    /// Applies `policy` if the mount holding `directory` has client-local locks. A filesystem is looked up once.
    /// Allow warns once, Error throws on every call.
    /// @throws eckit::UserError under LocalLockPolicy::Error.
    void validate(const eckit::LocalPathName& directory, LocalLockPolicy policy) const;

    std::unique_ptr<TocLock> holdForRead(int fd, const eckit::PathName& path) const override;

    std::unique_ptr<TocLock> holdForWrite(int fd, const eckit::PathName& path) const override;

    /// O_APPEND uses the client's cached file size, which may be stale. Seeking to the end revalidates it with the
    /// server, and positions the offset at the true end should the client not honour O_APPEND.
    /// Only correct under the lock from holdForWrite().
    void openedForAppend(int fd, const eckit::PathName& path) const override;

    bool excludesWriters() const override { return true; }

    eckit::PathName uniquePath(const eckit::PathName& path) const override;

private:  // members

    LocalLockLookup localLockLookup_;

    mutable std::mutex mutex_;
    /// What the lookup found, by device. Guarded by mutex_.
    mutable std::map<std::string, std::optional<std::string>> localLockOptions_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
