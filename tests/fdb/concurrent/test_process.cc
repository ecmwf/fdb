/*
 * (C) Copyright 2026- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   test_process.cc
/// @author Metin Cakircali
/// @date   Jul 2026

#include "test_common.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/ListIterator.h"

#include "eckit/runtime/Main.h"
#include "eckit/testing/ProcessFork.h"
#include "eckit/testing/Test.h"

#include <cstddef>


namespace fdb::test::concurrent {

//----------------------------------------------------------------------------------------------------------------------

CASE("Multi-process: archive") {
    const int count = process_count();

    TestFixture fixture(count);

    // Each worker archives (and flushes) its own disjoint slice concurrently.
    EXPECT(fork_and_exec(count, {"--fn=archive"}));

    // The union of all slices must be present and retrievable exactly.
    fdb5::FDB fdb;
    EXPECT_EQUAL(list_steps(fdb), expected_steps(count));
    for (int worker = 0; worker < count; ++worker) {
        for (int seq = 0; seq < k_seq_per_worker; ++seq) {
            EXPECT(retrieve_equals(fdb, make_key(worker, seq), make_data(worker, seq)));
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

CASE("Multi-process: retrieve") {
    const int count = process_count();

    TestFixture fixture(count);
    archive_all(count);

    // Each worker retrieves its own slice concurrently and verifies the bytes.
    EXPECT(fork_and_exec(count, {"--fn=retrieve"}));
}

//----------------------------------------------------------------------------------------------------------------------

CASE("Multi-process: inspect") {
    const int count = process_count();

    TestFixture fixture(count);
    archive_all(count);

    // Each worker inspects its own slice concurrently; each field matches exactly once.
    EXPECT(fork_and_exec(count, {"--fn=inspect"}));
}

//----------------------------------------------------------------------------------------------------------------------

CASE("Multi-process: list") {
    const int count = process_count();

    TestFixture fixture(count);
    archive_all(count);

    // Each worker lists its own slice concurrently; each field matches exactly once.
    EXPECT(fork_and_exec(count, {"--fn=list"}));

    // The parent sees the complete set of steps.
    fdb5::FDB fdb;
    EXPECT_EQUAL(list_steps(fdb), expected_steps(count));
}

//----------------------------------------------------------------------------------------------------------------------

CASE("Multi-process: axes") {
    const int count = process_count();

    TestFixture fixture(count);
    archive_all(count);

    // Each worker computes axes concurrently and verifies the full step set is visible.
    EXPECT(fork_and_exec(count, {"--fn=axes"}));

    fdb5::FDB fdb;
    EXPECT_EQUAL(axes_steps(fdb), expected_steps(count));
}

//----------------------------------------------------------------------------------------------------------------------

CASE("Multi-process: archive contention (same key)") {
    const int count = process_count();

    TestFixture fixture(count);

    // Every worker archives the identical field concurrently.
    EXPECT(fork_and_exec(count, {"--fn=contend"}));

    fdb5::FDB fdb;
    const auto request = contend_key().request();

    // Deduplicated view collapses to a single field...
    EXPECT_EQUAL(list_count(fdb, request, fdb5::ListMode::Deduplicate), 1U);
    // ...while the full view exposes one masked duplicate per worker.
    EXPECT_EQUAL(list_count(fdb, request, fdb5::ListMode::Full), static_cast<size_t>(count));
    // The retrievable field carries the expected payload.
    EXPECT(retrieve_equals(fdb, contend_key(), contend_data()));
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb::test::concurrent

int main(int argc, char** argv) {
    auto args = eckit::testing::parse_worker_args(argc, argv);
    if (!args.empty()) {
        eckit::Main::initialise(argc, argv);
        return fdb::test::concurrent::child_worker_main(argc, argv);
    }
    return eckit::testing::run_tests(argc, argv);
}

//----------------------------------------------------------------------------------------------------------------------
