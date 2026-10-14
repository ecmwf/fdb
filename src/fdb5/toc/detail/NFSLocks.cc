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

#include <cctype>
#include <map>
#include <mutex>
#include <optional>
#include <ostream>
#include <sstream>
#include <string>

#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/NFSMountInfo.h"
#include "eckit/log/CodeLocation.h"
#include "eckit/log/Log.h"
#include "eckit/utils/StringTools.h"

namespace fdb5::nfs {

//----------------------------------------------------------------------------------------------------------------------

static std::string logMessage(const eckit::LocalPathName& directory, const std::string& option) {
    std::ostringstream msg;
    msg << "NFS mount of " << directory << " has client-local locking (" << option << ")";
    return msg.str();
}

bool isMounted(const eckit::PathName& directory) {
    try {
        return directory.fileSystemType() == "nfs";
    }
    catch (const eckit::NotImplemented&) {
        return false;
    }
}

LockPolicy parseLockPolicy(const std::string& value) {
    const auto lowered = eckit::StringTools::lower(value);

    if (lowered == "warn") {
        return LockPolicy::Warn;
    }
    if (lowered == "error") {
        return LockPolicy::Error;
    }
    if (lowered == "ignore") {
        return LockPolicy::Ignore;
    }

    throw eckit::UserError("Invalid FDB_NFS_LOCAL_LOCK_POLICY=" + value, Here());
}

void enforceLockPolicy(LockPolicy policy, const eckit::LocalPathName& directory, const std::string& option) {
    switch (policy) {
        case LockPolicy::Ignore:
            return;
        case LockPolicy::Warn:
            eckit::Log::warning() << logMessage(directory, option) << std::endl;
            return;
        case LockPolicy::Error:
            throw eckit::UserError(logMessage(directory, option), Here());
    }
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
        enforceLockPolicy(policy, directory, *entry->second);
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::nfs
