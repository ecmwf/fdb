/*
 * (C) Copyright 2026- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the Horizon Europe programme funded project OpenCUBE
 * (Grant agreement: 101092984) horizon-opencube.eu
 */

/// @file   test_thread.cc
/// @author Metin Cakircali
/// @date   Jul 2026

#include "test_common.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/database/Key.h"

#include "eckit/testing/Test.h"

#include <atomic>
#include <cstddef>
#include <exception>
#include <functional>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace fdb::test::concurrent {

//----------------------------------------------------------------------------------------------------------------------

using ThreadWorker = std::function<int(int)>;

namespace {

/// @note eckit's testing EXPECT macros throw and are not designed for concurrent use.
/// thus, the worker threads record an int result (0 = success) into a results list.
/// all assertions run on the main thread after join().

/// Run @p worker on @p count threads, returning each thread's result (0 = success).
std::vector<int> run_threads(int count, const ThreadWorker& worker) {
    std::vector<int> results(static_cast<size_t>(count), -1);
    std::vector<std::thread> threads;
    threads.reserve(static_cast<size_t>(count));
    for (int id = 0; id < count; ++id) {
        threads.emplace_back([id, &results, &worker]() { results[static_cast<size_t>(id)] = worker(id); });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    return results;
}

/// Assert (on the main thread) that every worker thread succeeded.
void expect_workers_ok(const std::vector<int>& results) {
    for (int result : results) {
        EXPECT(result == 0);
    }
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

CASE("Multi-thread: archive (one FDB per thread)") {
    const int count = thread_count();

    TestFixture fixture(count);

    // Each thread owns its FDB and flushes only its own disjoint slice.
    expect_workers_ok(run_threads(count, [count](int id) { return worker_archive(id, count); }));

    fdb5::FDB fdb;
    EXPECT(list_steps(fdb) == expected_steps(count));
    for (int worker = 0; worker < count; ++worker) {
        for (int seq = 0; seq < k_seq_per_worker; ++seq) {
            EXPECT(retrieve_equals(fdb, make_key(worker, seq), make_data(worker, seq)));
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

CASE("Multi-thread: archive (shared FDB)") {
    const int count = thread_count();

    TestFixture fixture(count);

    // All threads archive into a single shared FDB
    fdb5::FDB shared;
    expect_workers_ok(run_threads(count, [&shared](int id) {
        for (int seq = 0; seq < k_seq_per_worker; ++seq) {
            const auto key = make_key(id, seq);
            const auto data = make_data(id, seq);
            shared.archive(key, static_cast<const void*>(data.data()), data.size());
        }
        return 0;
    }));

    // A single flush persists every thread's work.
    shared.flush();

    fdb5::FDB fdb;
    EXPECT(list_steps(fdb) == expected_steps(count));
    for (int worker = 0; worker < count; ++worker) {
        for (int seq = 0; seq < k_seq_per_worker; ++seq) {
            EXPECT(retrieve_equals(fdb, make_key(worker, seq), make_data(worker, seq)));
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

CASE("Multi-thread: retrieve") {
    const int count = thread_count();

    TestFixture fixture(count);
    archive_all(count);

    // Each thread retrieves its own slice through its own FDB and verifies the bytes.
    expect_workers_ok(run_threads(count, [count](int id) { return worker_retrieve(id, count); }));
}

//----------------------------------------------------------------------------------------------------------------------

CASE("Multi-thread: inspect") {
    const int count = thread_count();

    TestFixture fixture(count);
    archive_all(count);

    expect_workers_ok(run_threads(count, [count](int id) { return worker_inspect(id, count); }));
}

//----------------------------------------------------------------------------------------------------------------------

CASE("Multi-thread: list") {
    const int count = thread_count();

    TestFixture fixture(count);
    archive_all(count);

    expect_workers_ok(run_threads(count, [count](int id) { return worker_list(id, count); }));

    fdb5::FDB fdb;
    EXPECT(list_steps(fdb) == expected_steps(count));
}

//----------------------------------------------------------------------------------------------------------------------

CASE("Multi-thread: axes") {
    const int count = thread_count();

    TestFixture fixture(count);
    archive_all(count);

    // Every thread computes axes over the whole database and verifies the full step set.
    expect_workers_ok(run_threads(count, [count](int id) { return worker_axes(id, count); }));

    fdb5::FDB fdb;
    EXPECT(axes_steps(fdb) == expected_steps(count));
}

//----------------------------------------------------------------------------------------------------------------------

CASE("Multi-thread: archive contention (same key)") {
    const int count = thread_count();

    TestFixture fixture(count);

    // Every thread archives the identical field through its own FDB and flushes.
    expect_workers_ok(run_threads(count, [count](int id) { return worker_contend(id, count); }));

    fdb5::FDB fdb;
    const auto request = contend_key().request();

    // Deduplicated
    EXPECT_EQUAL(list_count(fdb, request, fdb5::ListMode::Deduplicate), 1U);
    // full view
    EXPECT_EQUAL(list_count(fdb, request, fdb5::ListMode::Full), static_cast<size_t>(count));
    // retrieve the single field and verify the bytes
    EXPECT(retrieve_equals(fdb, contend_key(), contend_data()));
}

//----------------------------------------------------------------------------------------------------------------------

CASE("Multi-thread: concurrent archive + flush (shared FDB)") {
    const int count = thread_count();

    TestFixture fixture(count);

    fdb5::FDB shared;

    // archiver threads
    std::vector<int> results(static_cast<size_t>(count), -1);
    std::vector<std::thread> archivers;
    archivers.reserve(static_cast<size_t>(count));
    for (int id = 0; id < count; ++id) {
        archivers.emplace_back([id, &shared, &results]() {
            int result = 0;
            try {
                for (int seq = 0; seq < k_seq_per_worker; ++seq) {
                    const auto key = make_key(id, seq);
                    const auto data = make_data(id, seq);
                    shared.archive(key, static_cast<const void*>(data.data()), data.size());
                }
            }
            catch (const std::exception& e) {
                std::cerr << "[thread] archive failed: " << e.what() << '\n';
                result = 1;
            }
            results[static_cast<size_t>(id)] = result;
        });
    }

    // Flusher threads
    std::atomic<bool> stop{false};
    std::atomic<bool> flush_ok{true};
    std::vector<std::thread> flushers;
    flushers.reserve(2);
    for (int f = 0; f < 2; ++f) {
        flushers.emplace_back([&shared, &stop, &flush_ok]() {
            while (!stop.load(std::memory_order_relaxed)) {
                try {
                    shared.flush();
                }
                catch (const std::exception& e) {
                    std::cerr << "[thread] flush failed: " << e.what() << '\n';
                    flush_ok.store(false, std::memory_order_relaxed);
                    return;
                }
                std::this_thread::yield();
            }
        });
    }

    for (auto& thread : archivers) {
        thread.join();
    }
    stop.store(true, std::memory_order_relaxed);
    for (auto& thread : flushers) {
        thread.join();
    }

    shared.flush();

    expect_workers_ok(results);
    EXPECT(flush_ok.load(std::memory_order_relaxed));

    fdb5::FDB fdb;
    EXPECT(list_steps(fdb) == expected_steps(count));
    for (int worker = 0; worker < count; ++worker) {
        for (int seq = 0; seq < k_seq_per_worker; ++seq) {
            EXPECT(retrieve_equals(fdb, make_key(worker, seq), make_data(worker, seq)));
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb::test::concurrent

int main(int argc, char** argv) {
    return eckit::testing::run_tests(argc, argv);
}

//----------------------------------------------------------------------------------------------------------------------
