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
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/database/Key.h"

#include "eckit/testing/Test.h"

#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace fdb::test::concurrent {

//----------------------------------------------------------------------------------------------------------------------

constexpr int k_flushers = 2;  // number of concurrent flusher threads
constexpr int k_max_archive = 1000;

using ThreadWorker = std::function<int(size_t)>;

namespace {

/// @note eckit's testing EXPECT macros throw and are not designed for concurrent use.
/// thus, the worker threads record an int result (0 = success) into a results list.
/// all assertions run on the main thread after join().

/// Run @p worker on @p count threads, returning each thread's result (0 = success).
std::vector<int> run_threads(const size_t count, const ThreadWorker& worker) {
    std::vector<int> results(count, -1);
    std::vector<std::thread> threads;
    threads.reserve(count);
    for (size_t id = 0; id < count; ++id) {
        threads.emplace_back([id, &results, &worker]() {
            try {
                results[id] = worker(id);
            }
            catch (const std::exception& error) {
                std::cerr << "[thread " << id << "] " << error.what() << '\n';
                results[id] = 1;
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    return results;
}

/// Assert (on the main thread) that every worker thread succeeded.
void expect_workers_ok(const std::vector<int>& results) {
    for (auto result : results) {
        EXPECT_EQUAL(result, 0);
    }
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

CASE("Multi-thread: archive (one FDB per thread)") {
    const auto count = thread_count();

    TestFixture fixture(count);

    // Each thread owns its FDB and flushes only its own disjoint slice.
    expect_workers_ok(run_threads(count, [](auto wid) { return worker_archive(wid); }));

    fdb5::FDB fdb;
    EXPECT_EQUAL(list_steps(fdb), expected_steps(count));
    for (int worker = 0; worker < count; ++worker) {
        for (int seq = 0; seq < k_seq_per_worker; ++seq) {
            EXPECT(retrieve_equals(fdb, make_key(worker, seq), make_data(worker, seq)));
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

CASE("Multi-thread: archive (shared FDB)") {
    const auto count = thread_count();

    TestFixture fixture(count);

    // All threads archive into a single shared FDB
    fdb5::FDB shared;
    expect_workers_ok(run_threads(count, [&shared](auto id) {
        for (int seq = 0; seq < k_seq_per_worker; ++seq) {
            const auto key = make_key(id, seq);
            const auto data = make_data(id, seq);
            shared.archive(key, static_cast<const void*>(data.data()), data.size());
        }
        return 0;
    }));

    shared.flush();

    fdb5::FDB fdb;
    EXPECT_EQUAL(list_steps(fdb), expected_steps(count));
    for (int worker = 0; worker < count; ++worker) {
        for (int seq = 0; seq < k_seq_per_worker; ++seq) {
            EXPECT(retrieve_equals(fdb, make_key(worker, seq), make_data(worker, seq)));
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

CASE("Multi-thread: retrieve") {
    const auto count = thread_count();

    TestFixture fixture(count);
    archive_all(count);

    expect_workers_ok(run_threads(count, [](auto wid) { return worker_retrieve(wid); }));
}

//----------------------------------------------------------------------------------------------------------------------

CASE("Multi-thread: inspect") {
    const auto count = thread_count();

    TestFixture fixture(count);
    archive_all(count);

    expect_workers_ok(run_threads(count, [](auto wid) { return worker_inspect(wid); }));
}

//----------------------------------------------------------------------------------------------------------------------

CASE("Multi-thread: list") {
    const auto count = thread_count();

    TestFixture fixture(count);
    archive_all(count);

    expect_workers_ok(run_threads(count, [](auto wid) { return worker_list(wid); }));

    fdb5::FDB fdb;
    EXPECT_EQUAL(list_steps(fdb), expected_steps(count));
}

//----------------------------------------------------------------------------------------------------------------------

CASE("Multi-thread: axes") {
    const auto count = thread_count();

    TestFixture fixture(count);
    archive_all(count);

    expect_workers_ok(run_threads(count, [count](auto) { return worker_axes(count); }));

    fdb5::FDB fdb;
    EXPECT_EQUAL(axes_steps(fdb), expected_steps(count));
}

//----------------------------------------------------------------------------------------------------------------------

CASE("Multi-thread: archive contention (same key)") {
    const auto count = thread_count();

    TestFixture fixture(count);

    expect_workers_ok(run_threads(count, [](auto) { return worker_contend(); }));

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
    const auto count = thread_count();

    TestFixture fixture(count);

    fdb5::FDB shared;

    std::atomic<size_t> flush_count{0};
    shared.registerFlushCallback([&flush_count]() { flush_count.fetch_add(1, std::memory_order_relaxed); });

    // Flusher threads
    std::atomic<bool> stop{false};
    std::atomic<bool> flush_ok{true};
    std::vector<std::thread> flushers;
    flushers.reserve(k_flushers);
    for (int f = 0; f < k_flushers; ++f) {
        flushers.emplace_back([&shared, &stop, &flush_ok]() {
            while (!stop.load(std::memory_order_relaxed)) {
                try {
                    shared.flush();
                }
                catch (const std::exception& error) {
                    std::cerr << "[thread] flush failed: " << error.what() << '\n';
                    flush_ok.store(false, std::memory_order_relaxed);
                    return;
                }
                std::this_thread::yield();
            }
        });
    }

    // Archivers: each keeps re-archiving its own (disjoint) slice until at least one real flush has
    // interleaved (bounded, so a broken flush fails the EXPECT below rather than spinning forever).
    // Re-archiving the same keys just masks the previous versions, so the final result is unchanged.
    const auto results = run_threads(count, [&shared, &flush_count](auto wid) {
        for (int round = 0; round < k_max_archive; ++round) {
            for (int seq = 0; seq < k_seq_per_worker; ++seq) {
                const auto key = make_key(wid, seq);
                const auto data = make_data(wid, seq);
                shared.archive(key, static_cast<const void*>(data.data()), data.size());
            }
            if (flush_count.load(std::memory_order_relaxed) > 0) {
                break;
            }
        }
        return 0;
    });

    stop.store(true, std::memory_order_relaxed);
    for (auto& thread : flushers) {
        thread.join();
    }

    shared.flush();

    expect_workers_ok(results);
    EXPECT(flush_ok.load(std::memory_order_relaxed));
    // check interleaved flushes
    EXPECT(flush_count.load(std::memory_order_relaxed) > 0);

    fdb5::FDB fdb;
    EXPECT_EQUAL(list_steps(fdb), expected_steps(count));
    for (int worker = 0; worker < count; ++worker) {
        for (int seq = 0; seq < k_seq_per_worker; ++seq) {
            EXPECT(retrieve_equals(fdb, make_key(worker, seq), make_data(worker, seq)));
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

CASE("Multi-thread: concurrent reads (shared FDB)") {
    const auto count = thread_count();

    TestFixture fixture(count);
    archive_all(count);

    fdb5::FDB shared;

    constexpr int k_rounds = 4;

    expect_workers_ok(run_threads(count, [&shared](auto id) {
        for (int round = 0; round < k_rounds; ++round) {
            for (int seq = 0; seq < k_seq_per_worker; ++seq) {
                const auto key = make_key(id, seq);
                if (!retrieve_equals(shared, key, make_data(id, seq))) {
                    return 1;
                }
                if (inspect_count(shared, key.request("retrieve")) != 1) {
                    return 1;
                }
                // NOTE: Concurrent list on a shared FDB is not safe until the following is addressed:
                // RootManager::fileSpaces() -> Config::getSubConfigurations() copies eckit::Value /
                // LocalConfiguration objects sharing a non-atomically reference-counted eckit::Counted.
                if (list_count(shared, key.request("list"), fdb5::ListMode::Deduplicate) != 1) {
                    return 1;
                }
            }
        }
        return 0;
    }));
}

//----------------------------------------------------------------------------------------------------------------------

CASE("Multi-thread: concurrent retrieve (shared FDB, shared ClientConnection)") {
    const auto threads = static_cast<size_t>(thread_count());
    const int iterations = env_int("TEST_FDB_RETRIEVE_ITERATIONS", 4);
    const bool config = (::getenv("FDB5_CONFIG_FILE") != nullptr) || (::getenv("FDB5_CONFIG") != nullptr);

    std::unique_ptr<TestFixture> fixture;
    if (!config) {
        fixture = std::make_unique<TestFixture>(static_cast<int>(threads));
        archive_all(static_cast<int>(threads));
    }

    struct Field {
        metkit::mars::MarsRequest request;
        std::string bytes;
    };

    // single-threaded, this is reference to test against
    std::vector<Field> fields;
    {
        fdb5::FDB fdb;
        auto iter = fdb.list(fdb5::FDBToolRequest({}, true, {}), /* deduplicate */ true);
        fdb5::ListElement elem;
        while (iter.next(elem)) {
            const auto request = elem.combinedKey().request("retrieve");
            std::unique_ptr<eckit::DataHandle> handle(fdb.retrieve(request));
            EXPECT(handle != nullptr);
            fields.push_back({request, read_handle(*handle)});
        }
    }
    EXPECT(!fields.empty());

    expect_workers_ok(run_threads(threads, [&fields, iterations](size_t) {
        fdb5::FDB fdb;
        for (int round = 0; round < iterations; ++round) {
            for (const auto& field : fields) {
                std::unique_ptr<eckit::DataHandle> handle(fdb.retrieve(field.request));
                if (!handle || read_handle(*handle) != field.bytes) {
                    return 1;
                }
            }
        }
        return 0;
    }));
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb::test::concurrent

int main(int argc, char** argv) {
    return eckit::testing::run_tests(argc, argv);
}

//----------------------------------------------------------------------------------------------------------------------
