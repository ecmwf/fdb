/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/toc/detail/NFSTocLocking.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <iomanip>
#include <ios>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>

#include "eckit/config/Configuration.h"
#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/NFSMountInfo.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/log/CodeLocation.h"
#include "eckit/log/Log.h"
#include "eckit/utils/StringTools.h"
#include "fdb5/toc/detail/TocLocking.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

namespace {

/// Blocking whole-file advisory lock.
// Prefer open-file-description locks if available, otherwise fall back to classic POSIX locks.
void fileLock(int fd, short type, const eckit::PathName& path) {
    struct flock lock{};
    lock.l_type = type;
    lock.l_whence = SEEK_SET;
    lock.l_start = 0;
    lock.l_len = 0;  // whole file

    // Default to classic POSIX locks.
    int cmd = F_SETLKW;
    // Attempt to use open-file-description locks (F_OFD_SETLKW) if available.
#ifdef F_OFD_SETLKW
    cmd = F_OFD_SETLKW;
#endif

    for (;;) {
        if (::fcntl(fd, cmd, &lock) == 0) {
            return;
        }
        if (errno == EINTR) {
            continue;  // interrupted by a signal, retry
        }
#ifdef F_OFD_SETLKW
        if (cmd == F_OFD_SETLKW && (errno == EINVAL || errno == ENOTSUP)) {
            cmd = F_SETLKW;  // OFD unsupported at runtime, degrade to classic locks
            continue;
        }
#endif
        const int err = errno;
        std::ostringstream msg;
        msg << "Failed to acquire advisory lock on TOC " << path.asString() << " (ensure NFS file locking is enabled).";
        throw eckit::FailedSystemCall(msg.str(), Here(), err);
    }
}

/// Serialises independent TOC handlers within a process when using process-scoped POSIX locks.
/// File identity is based on device and inode so aliases share the same mutex.
class ProcessLock final : public TocLock {

    using FileIdentity = std::tuple<pid_t, dev_t, ino_t>;

    struct Registry {
        std::mutex mutex;
        std::map<FileIdentity, std::weak_ptr<std::mutex>> locks;
    };

    static std::shared_ptr<std::mutex> mutexFor(int fd, const eckit::PathName& path) {
        static Registry registry;

        struct stat status{};
        SYSCALL2(::fstat(fd, &status), path);

        const std::scoped_lock lock{registry.mutex};
        auto& entry = registry.locks[FileIdentity{::getpid(), status.st_dev, status.st_ino}];
        auto file_mutex = entry.lock();
        if (!file_mutex) {
            file_mutex = std::make_shared<std::mutex>();
            entry = file_mutex;
        }
        return file_mutex;
    }

public:

    ProcessLock(int fd, const eckit::PathName& path) : mutex_{mutexFor(fd, path)}, lock_{*mutex_} {}

private:

    std::shared_ptr<std::mutex> mutex_;
    std::unique_lock<std::mutex> lock_;
};

std::optional<std::string> mount_option(const std::string& device) {
    return eckit::filesystem::nfs::local_lock_option(device);
}

std::string describe(const eckit::LocalPathName& directory, const std::string& option) {
    std::ostringstream message;
    message << "NFS mount of " << directory << " has client-local locking (" << option << ")";
    return message.str();
}

using Policy = NFSTocLocking::Policy;

constexpr std::array<std::pair<Policy, std::string_view>, 2> policy_names{
    {
        {Policy::Error, "error"},
        {Policy::Allow, "allow"},
    },
};

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

std::string to_string(Policy policy) {
    for (const auto& [value, name] : policy_names) {
        if (value == policy) {
            return std::string{name};
        }
    }
    NOTIMP;
}

std::ostream& operator<<(std::ostream& out, Policy policy) {
    return out << to_string(policy);
}

Policy NFSTocLocking::parsePolicy(const std::string& value, const std::string& source) {
    const auto lowered = eckit::StringTools::lower(value);

    for (const auto& [policy, name] : policy_names) {
        if (lowered == name) {
            return policy;
        }
    }

    std::ostringstream message;
    message << "Invalid " << source << "=" << value << " (expected ";
    for (size_t i = 0; i < policy_names.size(); ++i) {
        message << (i == 0 ? "" : ", ") << policy_names[i].second;
    }
    message << ")";
    throw eckit::UserError(message.str(), Here());
}

Policy NFSTocLocking::resolvePolicy(const eckit::Configuration& userConfig, Policy fallback) {
    const std::string fromResource =
        eckit::Resource<std::string>("fdbNfsLocalLockPolicy;$FDB_NFS_LOCAL_LOCK_POLICY", "");
    if (!fromResource.empty()) {
        return parsePolicy(fromResource, "FDB_NFS_LOCAL_LOCK_POLICY");
    }
    if (userConfig.has("nfsLocalLockPolicy")) {
        return parsePolicy(userConfig.getString("nfsLocalLockPolicy"), "nfsLocalLockPolicy");
    }
    return fallback;
}

NFSTocLocking::NFSTocLocking(Policy policy, Lookup lookup) : policy_{policy}, lookup_{std::move(lookup)} {}

NFSTocLocking::NFSTocLocking() : NFSTocLocking{Policy::Error, mount_option} {}

void NFSTocLocking::validate(const eckit::LocalPathName& directory, const eckit::Configuration& userConfig) const {
    validate(directory, resolvePolicy(userConfig, policy_));
}

void NFSTocLocking::validate(const eckit::LocalPathName& directory) const {
    validate(directory, policy_);
}

void NFSTocLocking::validate(const eckit::LocalPathName& directory, Policy policy) const {
    const std::string device = locateExistingDirectory(directory).device();

    const std::lock_guard lock{mutex_};

    auto entry = options_.find(device);
    const bool first_lookup = (entry == options_.end());
    if (first_lookup) {
        entry = options_.emplace(device, lookup_(device)).first;
    }

    const auto& option = entry->second;
    if (!option) {
        return;
    }

    if (policy == Policy::Error) {
        throw eckit::UserError(describe(directory, *option), Here());
    }

    if (first_lookup) {
        eckit::Log::warning() << describe(directory, *option) << std::endl;
    }
}

std::unique_ptr<TocLock> NFSTocLocking::holdForRead(int fd, const eckit::PathName& path) const {
    auto lock = std::make_unique<ProcessLock>(fd, path);
    fileLock(fd, F_RDLCK, path);
    return lock;
}

std::unique_ptr<TocLock> NFSTocLocking::holdForWrite(int fd, const eckit::PathName& path) const {
    auto lock = std::make_unique<ProcessLock>(fd, path);
    fileLock(fd, F_WRLCK, path);
    return lock;
}

eckit::PathName NFSTocLocking::uniquePath(const eckit::PathName& path) const {

    const auto unique = eckit::PathName::unique(path);

    // Another client can generate the same host, pid and time, so add a random token.
    // A fresh device per call: a shared generator would repeat its sequence in forked children.
    std::random_device device;
    const auto token = std::uniform_int_distribution<std::uint64_t>{}(device);

    std::ostringstream suffix;
    suffix << '.' << std::hex << std::setfill('0') << std::setw(16) << token;

    return unique + suffix.str();
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
