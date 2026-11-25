/*
 * (C) Copyright 2026- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"
#include "fdb5/toc/TocCommon.h"
#include "fdb5/toc/TocHandler.h"
#include "fdb5/toc/detail/NFSTocLocking.h"
#include "fdb5/toc/detail/TocLocking.h"

#include "eckit/config/LocalConfiguration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/NFSMountInfo.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/TmpDir.h"
#include "eckit/log/Log.h"
#include "eckit/testing/Test.h"

#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <future>
#include <optional>
#include <string>
#include <thread>
#include <typeinfo>
#include <vector>

#if defined(__SANITIZE_THREAD__)
#define FDB_TEST_WITH_THREAD_SANITIZER 1
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define FDB_TEST_WITH_THREAD_SANITIZER 1
#endif
#endif

namespace fdb5::test {

//----------------------------------------------------------------------------------------------------------------------

namespace {

// General CI can inherit a path from a harness whose mount is not visible in the test container.
// The dedicated NFS job must fail instead of silently skipping coverage in that situation.
std::optional<eckit::PathName> nfsMountDir() {
    const char* mount = ::getenv("FDB_TEST_NFS_MOUNT");
    std::string reason = "FDB_TEST_NFS_MOUNT is unset or empty";
    if (mount != nullptr && mount[0] != '\0') {
        const eckit::PathName directory{mount};
        reason = std::string("FDB_TEST_NFS_MOUNT=") + mount;
        if (!directory.exists() || !directory.isDir()) {
            reason += " is not an existing directory";
        }
        else {
            const std::string filesystem = directory.fileSystemType();
            if (filesystem == "nfs") {
                return directory;
            }
            reason += " is on " + filesystem + ", not NFS";
        }
    }
    const char* require_mount = ::getenv("FDB_TEST_NFS_REQUIRE_MOUNT");
    if (require_mount != nullptr && std::string(require_mount) == "1") {
        throw eckit::UserError(reason, Here());
    }
    eckit::Log::info() << reason << " -- skipping NFS-backed checks" << std::endl;
    return std::nullopt;
}

bool usesNFSLocking(const eckit::LocalPathName& directory) {
    return dynamic_cast<const NFSTocLocking*>(&TocLocking::forDirectory(directory)) != nullptr;
}

template <typename Worker>
bool runConcurrently(size_t thread_count, const Worker& worker) {
    std::atomic<bool> start{false};
    std::vector<int> results(thread_count, -1);
    std::vector<std::thread> threads;
    threads.reserve(thread_count);

    for (size_t thread = 0; thread < thread_count; ++thread) {
        threads.emplace_back([thread, &start, &results, &worker]() {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            try {
                worker(thread);
                results[thread] = 0;
            }
            catch (...) {
                results[thread] = 1;
            }
        });
    }

    start.store(true, std::memory_order_release);
    for (auto& thread : threads) {
        thread.join();
    }
    return std::all_of(results.begin(), results.end(), [](int result) { return result == 0; });
}

#if !defined(FDB_TEST_WITH_THREAD_SANITIZER)
template <typename Worker>
bool runInProcesses(size_t process_count, const Worker& worker) {
    std::vector<pid_t> processes;
    processes.reserve(process_count);

    for (size_t process = 0; process < process_count; ++process) {
        const pid_t pid = ::fork();
        if (pid == 0) {
            try {
                worker(process);
                ::_exit(0);
            }
            catch (...) {
                ::_exit(1);
            }
        }
        if (pid < 0) {
            break;  // Still reap the children already started.
        }
        processes.push_back(pid);
    }

    bool success = processes.size() == process_count;
    for (const pid_t pid : processes) {
        int status = 0;
        pid_t waited;
        do {
            waited = ::waitpid(pid, &status, 0);
        } while (waited == -1 && errno == EINTR);
        if (waited != pid || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            success = false;
        }
    }
    return success;
}
#endif

}  // namespace

CASE("TOC init/read round-trips on a main TOC") {
    const eckit::TmpDir dir;

    fdb5::Config config = fdb5::Config().expandConfig();
    fdb5::Key key{{{"class", "od"}, {"expver", "0001"}, {"stream", "oper"}}};

    {
        fdb5::TocHandler writer(dir, config);
        writer.writeInitRecord(key);
    }

    {
        fdb5::TocHandler reader(dir, config);
        EXPECT(reader.exists());
        EXPECT_EQUAL(reader.databaseKey(), key);
    }

    // A second creator must observe the initialised TOC rather than re-initialising it.
    {
        fdb5::TocHandler again(dir, config);
        again.writeInitRecord(key);
        EXPECT_EQUAL(again.databaseKey(), key);
    }
}

CASE("TOC read: a failed read is not cached") {
    const eckit::TmpDir dir;
    (dir / "toc").mkdir();  // opens for read, but cannot be read

    fdb5::Config config = fdb5::Config().expandConfig();
    fdb5::TocHandler handler(dir, config);

    const auto failure = [&handler]() -> std::string {
        try {
            handler.databaseKey();
        }
        catch (const eckit::Exception& error) {
            return typeid(error).name();
        }
        return {};
    };

    const std::string first = failure();
    EXPECT(!first.empty());
    EXPECT_EQUAL(failure(), first);  // the same failure again, not an error from a cache of the failed read
}

//----------------------------------------------------------------------------------------------------------------------

CASE("NFS locks: each policy is parsed from its name, case-insensitively") {
    using Policy = NFSTocLocking::LocalLockPolicy;

    EXPECT(NFSTocLocking::parsePolicy("error") == Policy::Error);
    EXPECT(NFSTocLocking::parsePolicy("ERROR") == Policy::Error);
    EXPECT(NFSTocLocking::parsePolicy("allow") == Policy::Allow);
    EXPECT(NFSTocLocking::parsePolicy("Allow") == Policy::Allow);
}

CASE("NFS locks: an unknown policy is rejected, naming its source and the accepted values") {
    for (const std::string value : {"", "warn", "ignore", "allow "}) {
        EXPECT_THROWS_AS(NFSTocLocking::parsePolicy(value), eckit::UserError);
    }

    try {
        NFSTocLocking::parsePolicy("strict", "nfsLocalLockPolicy");
        EXPECT(false);
    }
    catch (const eckit::UserError& error) {
        const std::string what = error.what();
        EXPECT(what.find("nfsLocalLockPolicy=strict") != std::string::npos);
        EXPECT(what.find("error, allow") != std::string::npos);
    }
}

CASE("NFS locks: the environment overrides the config, which overrides the default") {
    using Policy = NFSTocLocking::LocalLockPolicy;

    const char* saved = ::getenv("FDB_NFS_LOCAL_LOCK_POLICY");
    const std::optional<std::string> previous = saved ? std::optional<std::string>{saved} : std::nullopt;
    ::unsetenv("FDB_NFS_LOCAL_LOCK_POLICY");

    eckit::LocalConfiguration empty;
    eckit::LocalConfiguration allow;
    allow.set("nfsLocalLockPolicy", "allow");
    eckit::LocalConfiguration invalid;
    invalid.set("nfsLocalLockPolicy", "strict");

    EXPECT(NFSTocLocking::resolvePolicy(empty, Policy::Error) == Policy::Error);
    EXPECT(NFSTocLocking::resolvePolicy(allow, Policy::Error) == Policy::Allow);
    EXPECT_THROWS_AS(NFSTocLocking::resolvePolicy(invalid, Policy::Error), eckit::UserError);

    ::setenv("FDB_NFS_LOCAL_LOCK_POLICY", "error", 1);
    EXPECT(NFSTocLocking::resolvePolicy(empty, Policy::Allow) == Policy::Error);
    EXPECT(NFSTocLocking::resolvePolicy(allow, Policy::Allow) == Policy::Error);
    EXPECT(NFSTocLocking::resolvePolicy(invalid, Policy::Allow) == Policy::Error);

    const eckit::TmpDir tmp;
    const eckit::LocalPathName directory{tmp};
    const NFSTocLocking nfs{Policy::Error,
                            [](const std::string&) -> std::optional<std::string> { return "local_lock=posix"; }};
    EXPECT_THROWS_AS(nfs.validate(directory, allow), eckit::UserError);

    ::unsetenv("FDB_NFS_LOCAL_LOCK_POLICY");
    EXPECT_NO_THROW(nfs.validate(directory, allow));
    EXPECT_THROWS_AS(nfs.validate(directory, empty), eckit::UserError);

    if (previous) {
        ::setenv("FDB_NFS_LOCAL_LOCK_POLICY", previous->c_str(), 1);
    }
}

CASE("NFS locks: only the error policy throws, and only for client-local mounts") {
    using Policy = NFSTocLocking::LocalLockPolicy;

    const eckit::TmpDir tmp;  // the device of an existing directory is looked up
    const eckit::LocalPathName directory{tmp};

    const auto clientLocal = [](const std::string&) -> std::optional<std::string> { return "local_lock=posix"; };
    const auto shared = [](const std::string&) -> std::optional<std::string> { return std::nullopt; };

    EXPECT_NO_THROW(NFSTocLocking(Policy::Allow, clientLocal).validate(directory));
    EXPECT_THROWS_AS(NFSTocLocking(Policy::Error, clientLocal).validate(directory), eckit::UserError);

    EXPECT_NO_THROW(NFSTocLocking(Policy::Allow, shared).validate(directory));
    EXPECT_NO_THROW(NFSTocLocking(Policy::Error, shared).validate(directory));

    try {
        NFSTocLocking(Policy::Error, clientLocal).validate(directory);
        EXPECT(false);
    }
    catch (const eckit::UserError& error) {
        const std::string what = error.what();
        EXPECT(what.find(directory.path()) != std::string::npos);
        EXPECT(what.find("local_lock=posix") != std::string::npos);
    }
}

CASE("NFS locks: a filesystem is looked up once per locking") {
    using Policy = NFSTocLocking::LocalLockPolicy;

    const eckit::TmpDir tmp;
    const eckit::LocalPathName directory{tmp};

    int lookups = 0;
    const auto clientLocal = [&lookups](const std::string&) -> std::optional<std::string> {
        ++lookups;
        return "nolock";
    };

    const NFSTocLocking error{Policy::Error, clientLocal};
    EXPECT_THROWS_AS(error.validate(directory), eckit::UserError);
    EXPECT_THROWS_AS(error.validate(directory), eckit::UserError);  // not remembered as accepted
    EXPECT_EQUAL(lookups, 1);

    const NFSTocLocking allow{Policy::Allow, clientLocal};
    EXPECT_NO_THROW(allow.validate(directory));
    EXPECT_NO_THROW(allow.validate(directory));
    EXPECT_EQUAL(lookups, 2);
}

//----------------------------------------------------------------------------------------------------------------------

CASE("TOC locking: a local filesystem takes no locks, the NFS locking takes one per descriptor") {
    const eckit::TmpDir tmp;
    const eckit::LocalPathName directory{tmp};
    const eckit::LocalPathName file = directory / "toc";

    const int fd = ::open(file.localPath(), O_CREAT | O_RDWR, 0644);
    EXPECT(fd >= 0);

    if (!usesNFSLocking(directory)) {
        const TocLocking& local = TocLocking::forDirectory(directory);
        EXPECT(!local.excludesWriters());
        EXPECT(!local.holdForRead(fd, file));
        EXPECT(!local.holdForWrite(fd, file));
    }

    // Usable on any filesystem: the lookup is only needed to validate a directory.
    const NFSTocLocking nfs{NFSTocLocking::LocalLockPolicy::Allow,
                            [](const std::string&) -> std::optional<std::string> { return std::nullopt; }};
    EXPECT(nfs.excludesWriters());
    EXPECT(nfs.holdForWrite(fd, file));

    ::close(fd);
}

CASE("TOC locking: only the NFS locking moves an append descriptor to the end") {
    const eckit::TmpDir tmp;
    const eckit::LocalPathName directory{tmp};
    const eckit::LocalPathName file = directory / "toc";

    const int fd = ::open(file.localPath(), O_CREAT | O_RDWR | O_APPEND, 0644);
    EXPECT(fd >= 0);
    EXPECT_EQUAL(::write(fd, "0123456789", 10), 10);
    EXPECT_EQUAL(::lseek(fd, 0, SEEK_SET), 0);

    if (!usesNFSLocking(directory)) {
        TocLocking::forDirectory(directory).openedForAppend(fd, file);
        EXPECT_EQUAL(::lseek(fd, 0, SEEK_CUR), 0);
    }

    const NFSTocLocking nfs{NFSTocLocking::LocalLockPolicy::Allow,
                            [](const std::string&) -> std::optional<std::string> { return std::nullopt; }};
    nfs.openedForAppend(fd, file);
    EXPECT_EQUAL(::lseek(fd, 0, SEEK_CUR), 10);

    ::close(fd);
}

CASE("TOC locking: a directory that is not created yet counts as the filesystem it will be created on") {
    const eckit::TmpDir tmp;
    const eckit::LocalPathName directory{tmp};
    const eckit::LocalPathName missing = directory / "missing" / "deeper";

    EXPECT_EQUAL(&TocLocking::forDirectory(missing), &TocLocking::forDirectory(directory));
    EXPECT_NO_THROW(TocCommon::uniquePath(missing / "field"));

    const NFSTocLocking nfs{NFSTocLocking::LocalLockPolicy::Error,
                            [](const std::string&) -> std::optional<std::string> { return std::nullopt; }};
    EXPECT_NO_THROW(nfs.validate(missing));
}

//----------------------------------------------------------------------------------------------------------------------

CASE("NFS paths: only unique names on NFS get a random token") {
    const auto dots = [](const std::string& path) { return std::count(path.begin(), path.end(), '.'); };

    const eckit::TmpDir tmp;
    const eckit::LocalPathName local{tmp};
    if (!usesNFSLocking(local)) {
        const std::string name = fdb5::TocCommon::uniquePath(local / "field").asString();
        EXPECT_EQUAL(dots(name), dots(eckit::LocalPathName::unique(local / "field")));
    }

    const auto mount = nfsMountDir();
    if (!mount) {
        return;
    }

    const eckit::LocalPathName shared{mount->localPath()};
    const std::string first = fdb5::TocCommon::uniquePath(shared / "field").asString();
    const std::string second = fdb5::TocCommon::uniquePath(shared / "field").asString();

    // One more component than eckit's own name: the 16 digit hexadecimal token.
    EXPECT_EQUAL(dots(first), dots(eckit::LocalPathName::unique(shared / "field")) + 1);

    const std::string token = first.substr(first.rfind('.') + 1);
    EXPECT_EQUAL(token.size(), size_t{16});
    EXPECT(std::all_of(token.begin(), token.end(),
                       [](unsigned char character) { return std::isxdigit(character) && !std::isupper(character); }));
    EXPECT(first != second);
}

//----------------------------------------------------------------------------------------------------------------------

CASE("NFS mount: the harness export has shared locking") {
    const auto mount = nfsMountDir();
    if (!mount) {
        return;
    }

#if defined(__linux__)
    const eckit::LocalPathName directory{mount->localPath()};

    // A client-local mount would make the contention cases below pass without testing the locks.
    EXPECT(!eckit::filesystem::nfs::local_lock_option(directory.device()));

    // Shared locks must pass under every policy, including a second call served from the per-device cache.
    const TocLocking& locking = TocLocking::forDirectory(directory);
    EXPECT(dynamic_cast<const NFSTocLocking*>(&locking));
    EXPECT_NO_THROW(locking.validate(directory, eckit::LocalConfiguration{}));
    EXPECT_NO_THROW(locking.validate(directory, eckit::LocalConfiguration{}));
#endif
}

//----------------------------------------------------------------------------------------------------------------------

CASE("NFS mount: detection fires and a TOC round-trips on a real NFS export") {
    const auto mount = nfsMountDir();
    if (!mount) {
        return;
    }

    EXPECT(usesNFSLocking(eckit::LocalPathName{mount->localPath()}));

    const eckit::TmpDir dir{mount->localPath()};

    fdb5::Config config = fdb5::Config().expandConfig();
    fdb5::Key key{{{"class", "od"}, {"expver", "0001"}, {"stream", "oper"}}};

    {
        fdb5::TocHandler writer(dir, config);
        writer.writeInitRecord(key);
    }

    {
        fdb5::TocHandler reader(dir, config);
        EXPECT(reader.exists());
        EXPECT_EQUAL(reader.databaseKey(), key);
    }
}

CASE("NFS mount: the lock policy comes from the FDB config, which a sub toc is given") {
    const auto mount = nfsMountDir();
    if (!mount) {
        return;
    }

    const char* saved = ::getenv("FDB_NFS_LOCAL_LOCK_POLICY");
    const std::optional<std::string> previous = saved ? std::optional<std::string>{saved} : std::nullopt;
    ::unsetenv("FDB_NFS_LOCAL_LOCK_POLICY");

    const fdb5::Key key{{{"class", "od"}, {"expver", "0001"}, {"stream", "oper"}}};

    // The policy is parsed before the mount is looked at, so an invalid one shows which config was read.
    eckit::LocalConfiguration invalid;
    invalid.set("nfsLocalLockPolicy", "strict");

    const fdb5::Config valid = fdb5::Config().expandConfig();

    {
        const eckit::TmpDir dir{mount->localPath()};
        fdb5::Config config = valid;
        config.set("nfsLocalLockPolicy", "strict");
        fdb5::TocHandler handler(dir, config);
        EXPECT_THROWS_AS(handler.writeInitRecord(key), eckit::UserError);
    }

    {
        const eckit::TmpDir dir{mount->localPath()};
        fdb5::TocHandler handler(dir, fdb5::Config{valid, invalid});  // n.b. as the user config: not read
        EXPECT_NO_THROW(handler.writeInitRecord(key));
    }

    const eckit::TmpDir dir{mount->localPath()};

    fdb5::TocHandler configured(dir / "toc.configured", key, nullptr, &invalid);
    EXPECT_THROWS_AS(configured.writeInitRecord(key), eckit::UserError);

    fdb5::TocHandler unconfigured(dir / "toc.unconfigured", key);
    EXPECT_NO_THROW(unconfigured.writeInitRecord(key));

    if (previous) {
        ::setenv("FDB_NFS_LOCAL_LOCK_POLICY", previous->c_str(), 1);
    }
}

//----------------------------------------------------------------------------------------------------------------------

CASE("NFS mount: concurrent creators and writers preserve complete TOC records") {
    const auto mount = nfsMountDir();
    if (!mount) {
        return;
    }

    constexpr size_t thread_count = 8;
    constexpr size_t mixed_writer_count = 4;
    constexpr size_t records_per_mixed_writer = 25;
#if !defined(FDB_TEST_WITH_THREAD_SANITIZER)
    constexpr size_t process_count = 4;
    constexpr size_t records_per_process = 25;
#else
    constexpr size_t process_count = 0;
    constexpr size_t records_per_process = 0;
#endif
    constexpr size_t records_per_thread = 25;

    const eckit::TmpDir dir{mount->localPath()};

    const fdb5::Config config = fdb5::Config().expandConfig();
    const fdb5::Key key{{{"class", "od"}, {"expver", "0001"}, {"stream", "oper"}}};

    EXPECT(runConcurrently(thread_count, [&](size_t) {
        fdb5::TocHandler creator(dir, config);
        creator.writeInitRecord(key);
    }));

    EXPECT(runConcurrently(thread_count, [&](size_t) {
        fdb5::TocHandler writer(dir, config);
        for (size_t record = 0; record < records_per_thread; ++record) {
            writer.writeClearAllRecord();
        }
    }));

    EXPECT(runConcurrently(thread_count, [&](size_t thread) {
        if (thread < mixed_writer_count) {
            fdb5::TocHandler writer(dir, config);
            for (size_t record = 0; record < records_per_mixed_writer; ++record) {
                writer.writeClearAllRecord();
            }
        }
        else {
            for (size_t read = 0; read < records_per_mixed_writer; ++read) {
                fdb5::TocHandler reader(dir, config);
                EXPECT_EQUAL(reader.databaseKey(), key);
            }
        }
    }));

#if !defined(FDB_TEST_WITH_THREAD_SANITIZER)
    EXPECT(runInProcesses(process_count, [&](size_t) {
        fdb5::TocHandler writer(dir, config);
        for (size_t record = 0; record < records_per_process; ++record) {
            writer.writeClearAllRecord();
        }
    }));
#endif

    fdb5::TocHandler reader(dir, config);
    EXPECT_EQUAL(reader.databaseKey(), key);
    EXPECT_EQUAL(reader.numberOfRecords(), 1 + thread_count * records_per_thread +
                                               mixed_writer_count * records_per_mixed_writer +
                                               process_count * records_per_process);

    const eckit::PathName sub_toc_path = dir / "toc.sub";
    {
        fdb5::TocHandler sub_toc(sub_toc_path, key);
        sub_toc.writeInitRecord(key);
    }

    EXPECT(runConcurrently(thread_count, [&](size_t) {
        fdb5::TocHandler writer(sub_toc_path, key);
        for (size_t record = 0; record < records_per_thread; ++record) {
            writer.writeClearAllRecord();
        }
    }));

    fdb5::TocHandler sub_toc_reader(sub_toc_path, key);
    EXPECT_EQUAL(sub_toc_reader.databaseKey(), key);
    EXPECT_EQUAL(sub_toc_reader.numberOfRecords(), 1 + thread_count * records_per_thread);
}

//----------------------------------------------------------------------------------------------------------------------

CASE("NFS mount: a linked sub toc is read under its lock") {
    const auto mount = nfsMountDir();
    if (!mount) {
        return;
    }

    const eckit::TmpDir dir{mount->localPath()};

    const fdb5::Config config = fdb5::Config().expandConfig();
    const fdb5::Key key{{{"class", "od"}, {"expver", "0001"}, {"stream", "oper"}}};

    const eckit::PathName sub_toc_path = dir / "toc.sub";
    {
        fdb5::TocHandler toc(dir, config);
        toc.writeInitRecord(key);

        fdb5::TocHandler sub_toc(sub_toc_path, key);
        sub_toc.writeInitRecord(key);

        toc.writeSubTocRecord(sub_toc);
    }

    // Hold the sub toc as one of its writers does. A reader that walks the main TOC has to wait for it: reading
    // the sub toc without its lock would finish at once.
    const int fd = ::open(sub_toc_path.localPath(), O_RDWR);
    EXPECT(fd >= 0);
    auto lock = TocLocking::forDirectory(eckit::LocalPathName{dir.localPath()}).holdForWrite(fd, sub_toc_path);

    auto records = std::async(std::launch::async, [&] { return fdb5::TocHandler(dir, config).numberOfRecords(); });
    const auto status = records.wait_for(std::chrono::milliseconds(500));

    ::close(fd);
    lock.reset();  // after the close, as a TOC handler does

    EXPECT(status == std::future_status::timeout);

    // The init record and the link of the main TOC, then the init record of the sub toc.
    EXPECT_EQUAL(records.get(), size_t{3});
}

//----------------------------------------------------------------------------------------------------------------------

CASE("NFS mount: independent clients preserve records in a shared TOC") {
    const char* shared_dir = ::getenv("FDB_TEST_NFS_SHARED_DIR");
    if (shared_dir == nullptr || shared_dir[0] == '\0') {
        return;
    }

    const eckit::PathName dir{shared_dir};
    EXPECT(dir.exists());
    if (!dir.exists()) {
        return;
    }

    // On any other filesystem the clients would not share a TOC over NFS, and the counts below would still add up.
    EXPECT_EQUAL(dir.fileSystemType(), std::string("nfs"));
    EXPECT(usesNFSLocking(eckit::LocalPathName{dir.localPath()}));

    constexpr size_t records_per_client = 100;
    const char* clients_env = ::getenv("FDB_TEST_NFS_CLIENTS");
    const size_t client_count = clients_env == nullptr ? 2 : ::strtoul(clients_env, nullptr, 10);
    EXPECT(client_count > 0);

    fdb5::Config config = fdb5::Config().expandConfig();
    fdb5::Key key{{{"class", "od"}, {"expver", "0001"}, {"stream", "oper"}}};

    const char* role_env = ::getenv("FDB_TEST_NFS_ROLE");
    const std::string role = role_env == nullptr ? "write" : role_env;
    if (role == "verify") {
        fdb5::TocHandler reader(dir, config);
        EXPECT_EQUAL(reader.databaseKey(), key);
        EXPECT_EQUAL(reader.numberOfRecords(), 1 + client_count * records_per_client);
        return;
    }

    EXPECT_EQUAL(role, std::string("write"));
    fdb5::TocHandler creator(dir, config);
    creator.writeInitRecord(key);

    fdb5::TocHandler writer(dir, config);
    for (size_t record = 0; record < records_per_client; ++record) {
        writer.writeClearAllRecord();
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::test

int main(int argc, char** argv) {
    return ::eckit::testing::run_tests(argc, argv);
}
