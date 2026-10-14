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
#include <cstdlib>
#include <optional>
#include <string>
#include <thread>
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

/// Directory on a real NFS mount, as provided by the test harness (e.g. a docker nfs-server
/// export mounted at FDB_TEST_NFS_MOUNT). Empty when unset, so the NFS-backed cases self-skip in
/// environments without NFS (normal CI).
eckit::PathName nfsMountDir() {
    const char* mount = ::getenv("FDB_TEST_NFS_MOUNT");
    if (mount == nullptr || mount[0] == '\0') {
        return {};
    }
    return eckit::PathName{mount};
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

//----------------------------------------------------------------------------------------------------------------------

CASE("NFS locks: the policy is parsed case-insensitively and rejects unknown values") {
    using Policy = NFSTocLocking::Policy;

    EXPECT(NFSTocLocking::parsePolicy("warn") == Policy::Warn);
    EXPECT(NFSTocLocking::parsePolicy("error") == Policy::Error);
    EXPECT(NFSTocLocking::parsePolicy("ignore") == Policy::Ignore);
    EXPECT(NFSTocLocking::parsePolicy("Error") == Policy::Error);
    EXPECT(NFSTocLocking::parsePolicy("IGNORE") == Policy::Ignore);

    EXPECT_THROWS_AS(NFSTocLocking::parsePolicy(""), eckit::UserError);
    EXPECT_THROWS_AS(NFSTocLocking::parsePolicy("strict"), eckit::UserError);
    EXPECT_THROWS_AS(NFSTocLocking::parsePolicy("warn "), eckit::UserError);
}

CASE("NFS locks: only the error policy throws, and only for client-local mounts") {
    using Policy = NFSTocLocking::Policy;

    const eckit::TmpDir tmp;  // the device of an existing directory is looked up
    const eckit::LocalPathName directory{tmp};

    const auto clientLocal = [](const std::string&) -> std::optional<std::string> { return "local_lock=posix"; };
    const auto shared = [](const std::string&) -> std::optional<std::string> { return std::nullopt; };

    EXPECT_NO_THROW(NFSTocLocking(Policy::Warn, clientLocal).validate(directory));
    EXPECT_NO_THROW(NFSTocLocking(Policy::Ignore, clientLocal).validate(directory));
    EXPECT_THROWS_AS(NFSTocLocking(Policy::Error, clientLocal).validate(directory), eckit::UserError);

    EXPECT_NO_THROW(NFSTocLocking(Policy::Warn, shared).validate(directory));
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

CASE("NFS locks: a filesystem is looked up once, and not at all when ignored") {
    using Policy = NFSTocLocking::Policy;

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

    const NFSTocLocking warn{Policy::Warn, clientLocal};
    EXPECT_NO_THROW(warn.validate(directory));
    EXPECT_NO_THROW(warn.validate(directory));
    EXPECT_EQUAL(lookups, 2);

    const NFSTocLocking ignore{Policy::Ignore, clientLocal};
    EXPECT_NO_THROW(ignore.validate(directory));
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
        EXPECT(!local.lockForRead(fd, file));
        EXPECT(!local.lockForWrite(fd, file));
    }

    // Usable on any filesystem: the lookup is only needed to validate a directory.
    const NFSTocLocking nfs{NFSTocLocking::Policy::Ignore,
                            [](const std::string&) -> std::optional<std::string> { return std::nullopt; }};
    EXPECT(nfs.excludesWriters());
    EXPECT(nfs.lockForWrite(fd, file));

    ::close(fd);
}

CASE("TOC locking: a directory that is not created yet counts as the filesystem it will be created on") {
    const eckit::TmpDir tmp;
    const eckit::LocalPathName directory{tmp};
    const eckit::LocalPathName missing = directory / "missing" / "deeper";

    EXPECT_EQUAL(&TocLocking::forDirectory(missing), &TocLocking::forDirectory(directory));
    EXPECT_NO_THROW(TocCommon::uniquePath(missing / "field"));

    const NFSTocLocking nfs{NFSTocLocking::Policy::Error,
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

    const eckit::PathName mount = nfsMountDir();
    if (mount.path().empty() || !mount.exists()) {
        eckit::Log::info() << "NFS mount (FDB_TEST_NFS_MOUNT) not available -- skipping NFS part of unique path test"
                           << std::endl;
        return;
    }

    const eckit::LocalPathName shared{mount.localPath()};
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
    const eckit::PathName mount = nfsMountDir();
    if (mount.path().empty() || !mount.exists()) {
        eckit::Log::info() << "NFS mount (FDB_TEST_NFS_MOUNT) not available -- skipping NFS lock option test"
                           << std::endl;
        return;
    }

#if defined(__linux__)
    const eckit::LocalPathName directory{mount.localPath()};

    // A client-local mount would make the contention cases below pass without testing the locks.
    EXPECT(!eckit::filesystem::nfs::local_lock_option(directory.device()));

    // Shared locks must pass under every policy, including a second call served from the per-device cache.
    const TocLocking& locking = TocLocking::forDirectory(directory);
    EXPECT(dynamic_cast<const NFSTocLocking*>(&locking));
    EXPECT_NO_THROW(locking.validate(directory));
    EXPECT_NO_THROW(locking.validate(directory));
#endif
}

//----------------------------------------------------------------------------------------------------------------------

CASE("NFS mount: detection fires and a TOC round-trips on a real NFS export") {
    const eckit::PathName mount = nfsMountDir();
    if (mount.path().empty() || !mount.exists()) {
        eckit::Log::info() << "NFS mount (FDB_TEST_NFS_MOUNT) not available -- skipping NFS-backed test" << std::endl;
        return;
    }

    // The mount is present, so it must really be NFS; otherwise the harness is misconfigured
    // and this case would validate the wrong filesystem.
    EXPECT_EQUAL(mount.fileSystemType(), std::string("nfs"));
    EXPECT(usesNFSLocking(eckit::LocalPathName{mount.localPath()}));

    const eckit::TmpDir dir{mount.localPath()};

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

//----------------------------------------------------------------------------------------------------------------------

CASE("NFS mount: concurrent creators and writers preserve complete TOC records") {
    const eckit::PathName mount = nfsMountDir();
    if (mount.path().empty() || !mount.exists()) {
        eckit::Log::info() << "NFS mount (FDB_TEST_NFS_MOUNT) not available -- skipping NFS contention test"
                           << std::endl;
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

    const eckit::TmpDir dir{mount.localPath()};

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

CASE("NFS mount: independent clients preserve records in a shared TOC") {
    const char* shared_dir = ::getenv("FDB_TEST_NFS_SHARED_DIR");
    if (shared_dir == nullptr || shared_dir[0] == '\0') {
        return;
    }

    const eckit::PathName mount = nfsMountDir();
    EXPECT(!mount.path().empty());
    EXPECT_EQUAL(mount.fileSystemType(), std::string("nfs"));

    const eckit::PathName dir{shared_dir};
    EXPECT(dir.exists());
    if (!dir.exists()) {
        return;
    }

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
