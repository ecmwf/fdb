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
#include <iterator>
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

/// Blocks until `fd` holds an advisory lock of `type` (F_RDLCK or F_WRLCK) on its whole file.
/// Prefers open-file-description locks, and falls back to classic POSIX locks where there are none.
void lock_file(int fd, short type, const eckit::PathName& path) {
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

        const FileIdentity identity{::getpid(), status.st_dev, status.st_ino};

        const std::scoped_lock lock{registry.mutex};

        if (const auto found = registry.locks.find(identity); found != registry.locks.end()) {
            if (auto fileMutex = found->second.lock()) {
                return fileMutex;
            }
        }

        // An entry expires once no handler holds its file, so drop those: the registry stays as small as the number
        // of files locked right now, however many TOCs a long-running process goes through.
        for (auto entry = registry.locks.begin(); entry != registry.locks.end();) {
            entry = entry->second.expired() ? registry.locks.erase(entry) : std::next(entry);
        }

        auto fileMutex = std::make_shared<std::mutex>();
        registry.locks[identity] = fileMutex;
        return fileMutex;
    }

public:

    ProcessLock(int fd, const eckit::PathName& path) : mutex_{mutexFor(fd, path)}, lock_{*mutex_} {}

private:

    std::shared_ptr<std::mutex> mutex_;
    std::unique_lock<std::mutex> lock_;
};

/// The process lock first, so that only one handler of this process asks for the file lock.
std::unique_ptr<TocLock> hold_locks(int fd, short type, const eckit::PathName& path) {
    auto lock = std::make_unique<ProcessLock>(fd, path);
    lock_file(fd, type, path);
    return lock;
}

std::optional<std::string> system_local_lock_option(const std::string& device) {
    return eckit::filesystem::nfs::local_lock_option(device);
}

std::string client_local_locks_message(const eckit::LocalPathName& directory, const std::string& option) {
    std::ostringstream message;
    message << "NFS mount of " << directory << " has client-local locking (" << option << ")";
    return message.str();
}

using LocalLockPolicy = NFSTocLocking::LocalLockPolicy;

constexpr std::array<std::pair<LocalLockPolicy, std::string_view>, 2> policyNames{
    {
        {LocalLockPolicy::Error, "error"},
        {LocalLockPolicy::Allow, "allow"},
    },
};

/// The random token of a unique name is written as this many hexadecimal digits.
constexpr int tokenHexDigits = 2 * sizeof(std::uint64_t);

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

LocalLockPolicy NFSTocLocking::parsePolicy(const std::string& value, const std::string& source) {
    const auto lowered = eckit::StringTools::lower(value);

    for (const auto& [policy, name] : policyNames) {
        if (lowered == name) {
            return policy;
        }
    }

    std::ostringstream message;
    message << "Invalid " << source << "=" << value << " (expected ";
    const char* separator = "";
    for (const auto& [policy, name] : policyNames) {
        message << separator << name;
        separator = ", ";
    }
    message << ")";
    throw eckit::UserError(message.str(), Here());
}

LocalLockPolicy NFSTocLocking::resolvePolicy(const eckit::Configuration& config) {
    const std::string fromResource =
        eckit::Resource<std::string>("fdbNfsLocalLockPolicy;$FDB_NFS_LOCAL_LOCK_POLICY", "");
    if (!fromResource.empty()) {
        return parsePolicy(fromResource, "FDB_NFS_LOCAL_LOCK_POLICY");
    }
    if (config.has("nfsLocalLockPolicy")) {
        return parsePolicy(config.getString("nfsLocalLockPolicy"), "nfsLocalLockPolicy");
    }
    return LocalLockPolicy::Error;
}

NFSTocLocking::NFSTocLocking(LocalLockLookup lookup) : localLockLookup_{std::move(lookup)} {}

NFSTocLocking::NFSTocLocking() : NFSTocLocking{system_local_lock_option} {}

void NFSTocLocking::validate(const eckit::LocalPathName& directory, const eckit::Configuration& config) const {
    validate(directory, resolvePolicy(config));
}

void NFSTocLocking::validate(const eckit::LocalPathName& directory, LocalLockPolicy policy) const {
    const std::string device = locateExistingDirectory(directory).device();

    const std::scoped_lock lock{mutex_};

    auto entry = localLockOptions_.find(device);
    const bool firstLookup = (entry == localLockOptions_.end());
    if (firstLookup) {
        entry = localLockOptions_.emplace(device, localLockLookup_(device)).first;
    }

    const auto& option = entry->second;
    if (!option) {
        return;
    }

    if (policy == LocalLockPolicy::Error) {
        throw eckit::UserError(client_local_locks_message(directory, *option), Here());
    }

    if (firstLookup) {
        eckit::Log::warning() << client_local_locks_message(directory, *option) << std::endl;
    }
}

std::unique_ptr<TocLock> NFSTocLocking::holdForRead(int fd, const eckit::PathName& path) const {
    return hold_locks(fd, F_RDLCK, path);
}

std::unique_ptr<TocLock> NFSTocLocking::holdForWrite(int fd, const eckit::PathName& path) const {
    return hold_locks(fd, F_WRLCK, path);
}

void NFSTocLocking::openedForAppend(int fd, const eckit::PathName& path) const {
    SYSCALL2(::lseek(fd, 0, SEEK_END), path);
}

eckit::PathName NFSTocLocking::uniquePath(const eckit::PathName& path) const {

    const auto unique = eckit::PathName::unique(path);

    // Another client can generate the same host, pid and time, so add a random token.
    // A fresh device per call: a shared generator would repeat its sequence in forked children.
    std::random_device device;
    const auto token = std::uniform_int_distribution<std::uint64_t>{}(device);

    std::ostringstream suffix;
    suffix << '.' << std::hex << std::setfill('0') << std::setw(tokenHexDigits) << token;

    return unique + suffix.str();
}

eckit::PathName NFSTocLocking::uniqueSubTocPath(const eckit::LocalPathName& directory) const {
    return uniquePath(directory / "toc");
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
