/*
 * (C) Copyright 2026- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/exception/Exceptions.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"
#include "fdb5/toc/TocHandler.h"

#include "eckit/filesystem/PathName.h"
#include "eckit/log/Log.h"
#include "eckit/testing/Test.h"

#include <sched.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <ostream>
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

bool onNFS(const eckit::PathName& path) {
    try {
        return path.fileSystemType() == "nfs";
    }
    catch (const eckit::FailedSystemCall& e) {
        eckit::Log::debug() << "Could not determine the filesystem backing " << path << " -- " << e.what() << std::endl;
        return false;
    }
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
            return false;
        }
        processes.push_back(pid);
    }

    bool success = true;
    for (const pid_t pid : processes) {
        int status = 0;
        while (::waitpid(pid, &status, 0) < 0 && errno == EINTR) {}
        success = success && WIFEXITED(status) && WEXITSTATUS(status) == 0;
    }
    return success;
}

}  // namespace

CASE("Detection matches the filesystem the directory actually resides on") {
    eckit::PathName tmp = eckit::PathName::unique("nfs_detect_dir");
    tmp.mkdir();

    EXPECT(onNFS(tmp) == (tmp.fileSystemType() == "nfs"));

    tmp.rmdir();
}

CASE("A non-existent path fails open to non-NFS") {
    eckit::PathName missing = eckit::PathName::unique("nfs_missing_dir");
    EXPECT(onNFS(missing) == false);
}

CASE("TOC init/read round-trips on a main TOC") {
    // n.b. the (path, parentKey) overload constructs a *sub* TOC, and every NFS branch is
    // guarded by !isSubToc_; the (directory, config) overload below is the main-TOC path,
    // so this exercises the code the NFS handling actually applies to.
    eckit::PathName dir = eckit::PathName::unique("nfs_toc_dir");
    dir.mkdir();

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

    (dir / "toc").unlink();
    (dir / "schema").unlink();
    dir.rmdir();
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

    eckit::PathName dir = eckit::PathName::unique(mount / "fdb_nfs_test");
    dir.mkdir();

    // Detection must fire on a genuine NFS mount (the local unit case above only checks agreement).
    EXPECT(onNFS(dir));

    fdb5::Config config = fdb5::Config().expandConfig();
    fdb5::Key key{{{"class", "od"}, {"expver", "0001"}, {"stream", "oper"}}};

    // Exercises the NFS branch (onNFS() && !isSubToc_): locked append on write, locked read on open.
    {
        fdb5::TocHandler writer(dir, config);
        writer.writeInitRecord(key);
    }

    {
        fdb5::TocHandler reader(dir, config);
        EXPECT(reader.exists());
        EXPECT_EQUAL(reader.databaseKey(), key);
    }

    (dir / "toc").unlink();
    (dir / "schema").unlink();
    dir.rmdir();
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
#if !defined(FDB_TEST_WITH_THREAD_SANITIZER)
    constexpr size_t process_count = 4;
    constexpr size_t records_per_process = 25;
#else
    constexpr size_t process_count = 0;
    constexpr size_t records_per_process = 0;
#endif
    constexpr size_t records_per_thread = 25;

    const eckit::PathName dir = eckit::PathName::unique(mount / "fdb_nfs_contention");
    dir.mkdir();

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
    EXPECT_EQUAL(reader.numberOfRecords(), 1 + thread_count * records_per_thread + process_count * records_per_process);

    (dir / "toc").unlink();
    (dir / "schema").unlink();
    dir.rmdir();
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::test

int main(int argc, char** argv) {
    return ::eckit::testing::run_tests(argc, argv);
}
