/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/toc/detail/NFSLocks.h"

#include <algorithm>
#include <cctype>
#include <map>
#include <mutex>
#include <optional>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>

#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/NFSMountInfo.h"
#include "eckit/log/CodeLocation.h"
#include "eckit/log/Log.h"

namespace fdb5::nfs {

//----------------------------------------------------------------------------------------------------------------------

LockPolicy parseLockPolicy(std::string_view value) {
    std::string lowered{value};
    std::transform(lowered.begin(), lowered.end(), lowered.begin(),
                   [](unsigned char character) { return static_cast<char>(std::tolower(character)); });

    if (lowered == "warn") {
        return LockPolicy::Warn;
    }
    if (lowered == "error") {
        return LockPolicy::Error;
    }
    if (lowered == "ignore") {
        return LockPolicy::Ignore;
    }

    throw eckit::UserError(
        "Invalid FDB_NFS_LOCAL_LOCK_POLICY '" + std::string{value} + "': expected warn, error or ignore", Here());
}

void reportLocalLocks(LockPolicy policy, const eckit::LocalPathName& directory, std::string_view option) {
    if (policy == LockPolicy::Ignore) {
        return;
    }

    std::ostringstream message;
    message << "NFS mount of " << directory << " has client-local locking (" << option
            << "): FDB TOC locks do not protect against writers on other clients";

    if (policy == LockPolicy::Error) {
        message << " (FDB_NFS_LOCAL_LOCK_POLICY=error)";
        throw eckit::UserError(message.str(), Here());
    }

    eckit::Log::warning() << message.str() << std::endl;
}

void enforceLockPolicy(const eckit::LocalPathName& directory) {
    static const LockPolicy policy = [] {
        const std::string value =
            eckit::Resource<std::string>("fdbNfsLocalLockPolicy;$FDB_NFS_LOCAL_LOCK_POLICY", "warn");
        return parseLockPolicy(value);
    }();

    if (policy == LockPolicy::Ignore) {
        return;
    }

    const auto device = directory.device();

    static std::mutex mutex;
    static std::map<std::string, std::optional<std::string>> clientLocalOptions;

    const std::lock_guard lock{mutex};

    auto entry = clientLocalOptions.find(device);
    const bool inserted = entry == clientLocalOptions.end();
    if (inserted) {
        entry = clientLocalOptions.emplace(device, eckit::filesystem::nfs::local_lock_option(device)).first;
    }

    if (entry->second && (inserted || policy == LockPolicy::Error)) {
        reportLocalLocks(policy, directory, *entry->second);
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::nfs
