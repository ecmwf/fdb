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

#include <string_view>

#include "eckit/filesystem/LocalPathName.h"

namespace fdb5::nfs {

//----------------------------------------------------------------------------------------------------------------------

/// What to do when an NFS mount has client-local locks.
enum class LockPolicy {
    Warn,   ///< log a warning, once per filesystem
    Error,  ///< throw eckit::UserError
    Ignore  ///< do nothing
};

/// Parses "warn", "error" or "ignore" (case-insensitive).
/// @throws eckit::UserError for any other value.
LockPolicy parseLockPolicy(std::string_view value);

/// Applies `policy` to a mount of `directory` whose locks are client-local because of `option`.
/// @throws eckit::UserError under LockPolicy::Error.
void reportLocalLocks(LockPolicy policy, const eckit::LocalPathName& directory, std::string_view option);

/// Applies the policy in $FDB_NFS_LOCAL_LOCK_POLICY (default "warn") if the NFS mount holding `directory` has
/// client-local locks, as reported by eckit::filesystem::nfs::local_lock_option(). Linux only: elsewhere nothing
/// is detected, so no policy has an effect.
///
/// Warn reports once per filesystem; Error throws on every call, so a failed open can be retried only after fixing
/// the mount.
/// @throws eckit::UserError under the "error" policy, or if the variable holds an unknown value.
void enforceLockPolicy(const eckit::LocalPathName& directory);

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::nfs
