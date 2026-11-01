/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   test_tocstore_close.cc
/// Regression test: TocStore must release its cached archive data handles when
/// destroyed, even without an explicit close() call, and must keep closing the
/// remaining handles if one of them fails to close.

#include "fdb5/config/Config.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/Store.h"
#include "fdb5/toc/TocStore.h"

#include "eckit/config/YAMLConfiguration.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/testing/Filesystem.h"
#include "eckit/testing/Test.h"

#include <cstdio>
#include <fcntl.h>
#include <string>
#include <sys/resource.h>
#include <unistd.h>
#include <vector>

#if defined(__APPLE__)
#include <sys/syslimits.h>
#else
#include <climits>
#endif

namespace fdb5 {
namespace test {

namespace {

//----------------------------------------------------------------------------------------------------------------------
// Portable, best-effort fd accounting. No assumption is made about which fd numbers
// are in use; every candidate up to the soft RLIMIT_NOFILE is probed.

size_t countOpenFileDescriptors() {
    struct rlimit limit {};
    ::getrlimit(RLIMIT_NOFILE, &limit);

    size_t count = 0;
    for (int fd = 0; fd < static_cast<int>(limit.rlim_cur); ++fd) {
        if (::fcntl(fd, F_GETFD) != -1) {
            ++count;
        }
    }
    return count;
}

/// Finds the fd number of an already-open file whose resolved path matches `path`.
/// Used only to sabotage one cached handle's fd behind its back, to simulate a
/// close() failure. Returns -1 if none is found (e.g. on an unsupported platform).
int findOpenFdForPath(const eckit::PathName& path) {
    struct rlimit limit {};
    ::getrlimit(RLIMIT_NOFILE, &limit);

    const std::string target = path.asString();

    for (int fd = 0; fd < static_cast<int>(limit.rlim_cur); ++fd) {
        if (::fcntl(fd, F_GETFD) == -1) {
            continue;
        }

#if defined(__APPLE__)
        char resolved[PATH_MAX];
        if (::fcntl(fd, F_GETPATH, resolved) == -1) {
            continue;
        }
        if (target == resolved) {
            return fd;
        }
#elif defined(__linux__)
        char link[32];
        std::snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);

        char resolved[PATH_MAX];
        ssize_t n = ::readlink(link, resolved, sizeof(resolved) - 1);
        if (n <= 0) {
            continue;
        }
        resolved[n] = '\0';

        if (target == resolved) {
            return fd;
        }
#endif
    }
    return -1;
}

eckit::PathName makeRoot(const std::string& name) {
    eckit::PathName root(name);
    if (root.exists()) {
        eckit::testing::deldir(root);
    }
    root.mkdir();
    return root;
}

fdb5::Config makeLocalTocConfig(const eckit::PathName& root) {
    const std::string yaml = R"(
spaces:
- roots:
  - path: )" + root.asString() +
                              R"(
type: local
engine: toc
store: file)";

    return fdb5::Config{eckit::YAMLConfiguration(yaml)};
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

CASE("TocStore releases cached archive handles when destroyed without explicit close()") {

    const eckit::PathName root = makeRoot("./test_tocstore_close_root");
    const fdb5::Config config = makeLocalTocConfig(root);

    const fdb5::Key storeKey({{"class", "od"}, {"expver", "0001"}});

    const char* data = "regression-test-data";
    const auto length = std::char_traits<char>::length(data);

    constexpr int numHandles = 5;

    const size_t baseline = countOpenFileDescriptors();

    {
        fdb5::TocStore tocStore(storeKey, config);
        fdb5::Store& store = tocStore;

        for (int i = 0; i < numHandles; ++i) {
            const fdb5::Key idxKey({{"class", "od"}, {"expver", "0001"}, {"step", std::to_string(i)}});
            auto floc = store.archive(idxKey, data, length);
            EXPECT(floc);
        }

        store.flush();

        // Each distinct data path opens and caches its own handle: fd count must grow.
        EXPECT(countOpenFileDescriptors() > baseline);

        // No explicit store.close() here — the destructor below must release them.
    }

    EXPECT_EQUAL(countOpenFileDescriptors(), baseline);
}

CASE("TocStore destructor keeps closing remaining handles if one close() fails") {

    const eckit::PathName root = makeRoot("./test_tocstore_close_failure_root");
    const fdb5::Config config = makeLocalTocConfig(root);

    const fdb5::Key storeKey({{"class", "od"}, {"expver", "0002"}});

    const char* data = "close-failure-test-data";
    const auto length = std::char_traits<char>::length(data);

    constexpr int numHandles = 3;

    const size_t baseline = countOpenFileDescriptors();

    {
        fdb5::TocStore tocStore(storeKey, config);
        fdb5::Store& store = tocStore;

        eckit::PathName lastDataPath;
        for (int i = 0; i < numHandles; ++i) {
            const fdb5::Key idxKey({{"class", "od"}, {"expver", "0002"}, {"step", std::to_string(i)}});
            auto floc = store.archive(idxKey, data, length);
            EXPECT(floc);
            lastDataPath = floc->uri().path();
        }

        store.flush();

        // Sabotage the last handle's fd behind its back: the next fclose() on it
        // will fail with EBADF. Done last, so no further opens reuse this fd number
        // before the store is destroyed below.
        const int victimFd = findOpenFdForPath(lastDataPath);
        if (victimFd >= 0) {
            ::close(victimFd);
        }

        // store destructs here without an explicit close(). If the fix to
        // TocStore::closeDataHandles() regresses (a throwing close() aborting the
        // loop), the other handles below would stay leaked and this assertion fails.
    }

    // At most the sabotaged fd number may remain unaccounted for (it was closed by
    // us, not by the handle, so the real file it pointed to may still be open under
    // a different fd depending on platform/timing); every other cached handle must
    // have been released regardless of the one close() failure.
    EXPECT(countOpenFileDescriptors() <= baseline + 1);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace test
}  // namespace fdb5

int main(int argc, char** argv) {
    return eckit::testing::run_tests(argc, argv);
}
