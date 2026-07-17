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

/// @file   test_common.h
/// @author Metin Cakircali
/// @date   Jul 2026

#pragma once

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/database/IndexAxis.h"
#include "fdb5/database/Key.h"

#include "metkit/mars/MarsRequest.h"

#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/TmpDir.h"
#include "eckit/io/DataHandle.h"
#include "eckit/testing/ProcessFork.h"
#include "eckit/testing/Test.h"

#include <array>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <set>
#include <string>

namespace fdb::test::concurrent {

//----------------------------------------------------------------------------------------------------------------------

using eckit::testing::fork_and_exec;
using eckit::testing::get_worker_arg;
using eckit::testing::parse_worker_args;

//----------------------------------------------------------------------------------------------------------------------

// Concurrent workers (processes or threads) write into a single, shared, isolated FDB root. Each
// worker owns a disjoint slice of the key space so the expected result set is fully deterministic:
// the base key is fixed and every worker varies the "step" it archives via make_step(worker, seq).

/// Number of fields each worker archives into its own (disjoint) slice.
constexpr int k_seq_per_worker = 3;

/// Stride separating the step ranges owned by consecutive workers.
constexpr int k_worker_step_stride = 100;

/// Read an integer environment variable, falling back to @p fallback if unset/invalid.
inline int env_int(const char* name, int fallback) {
    if (const char* value = ::getenv(name)) {
        try {
            return std::stoi(value);
        }
        catch (...) {
            return fallback;
        }
    }
    return fallback;
}

inline int process_count() {
    return env_int("TEST_FDB_PROCESS_COUNT", 8);
}

inline int thread_count() {
    return env_int("TEST_FDB_THREAD_COUNT", 8);
}

/// Absolute path to the test schema (configured by CMake under $FDB_HOME/etc/fdb/schema).
inline std::string schema_path() {
    const char* home = ::getenv("FDB_HOME");
    return std::string(home ? home : ".") + "/etc/fdb/schema";
}

/// Build an inline FDB configuration (FDB5_CONFIG) pointing at an isolated @p root directory.
inline std::string make_config_yaml(const std::string& root) {
    return std::string("---\n") + "type: local\n" + "engine: toc\n" + "schema: " + schema_path() + "\n" + "spaces:\n" +
           "- handler: Default\n" + "  roots:\n" + "  - path: " + root + "\n";
}

/// The fixed part of the key shared by every archived field (includes "param", omits "step").
inline fdb5::Key make_base_key() {
    fdb5::Key key;
    key.set("class", "od");
    key.set("expver", "0001");
    key.set("type", "fc");
    key.set("stream", "oper");
    key.set("date", "20101010");
    key.set("time", "0000");
    key.set("domain", "g");
    key.set("levtype", "sfc");
    key.set("param", "130");
    return key;
}

/// Deterministic, worker-unique step value.
inline std::string make_step(int worker_id, int seq) {
    return std::to_string((worker_id * k_worker_step_stride) + seq);
}

/// Fully specified datum key for a single field owned by @p worker_id.
inline fdb5::Key make_key(int worker_id, int seq) {
    auto key = make_base_key();
    key.set("step", make_step(worker_id, seq));
    return key;
}

/// Payload for a single field, encoding its owning worker/seq so retrieves can be verified exactly.
inline std::string make_data(int worker_id, int seq) {
    return "worker=" + std::to_string(worker_id) + ";seq=" + std::to_string(seq);
}

/// The full set of steps expected once every worker has archived its slice.
inline std::set<std::string> expected_steps(int count) {
    std::set<std::string> steps;
    for (int worker = 0; worker < count; ++worker) {
        for (int seq = 0; seq < k_seq_per_worker; ++seq) {
            steps.insert(make_step(worker, seq));
        }
    }
    return steps;
}

/// Shared key/data used by the write-contention cases (all workers write the identical field).
inline fdb5::Key contend_key() {
    auto key = make_base_key();
    key.set("step", "9900");
    return key;
}

inline std::string contend_data() {
    return "contended-field";
}

//----------------------------------------------------------------------------------------------------------------------
// Verification helpers (usable from both the parent test and the workers)

/// Drain a DataHandle into a string.
inline std::string read_handle(eckit::DataHandle& handle) {
    handle.openForRead();
    eckit::AutoClose closer(handle);
    std::string out;
    std::array<char, 1024> buffer{};
    long len = 0;
    while ((len = handle.read(buffer.data(), buffer.size())) > 0) {
        out.append(buffer.data(), static_cast<size_t>(len));
    }
    return out;
}

/// Retrieve a single field and compare its bytes against @p expected.
inline bool retrieve_equals(fdb5::FDB& fdb, const fdb5::Key& key, const std::string& expected) {
    std::unique_ptr<eckit::DataHandle> handle(fdb.retrieve(key.request()));
    if (!handle) {
        return false;
    }
    return read_handle(*handle) == expected;
}

/// Count the elements returned by list() for a request.
inline size_t list_count(fdb5::FDB& fdb, const metkit::mars::MarsRequest& request, fdb5::ListMode mode) {
    fdb5::FDBToolRequest tool_request(request);
    auto iter = fdb.list(tool_request, mode);
    fdb5::ListElement elem;
    size_t count = 0;
    while (iter.next(elem)) {
        ++count;
    }
    return count;
}

/// Count the elements returned by inspect() for a request.
inline size_t inspect_count(fdb5::FDB& fdb, const metkit::mars::MarsRequest& request) {
    auto iter = fdb.inspect(request);
    fdb5::ListElement elem;
    size_t count = 0;
    while (iter.next(elem)) {
        ++count;
    }
    return count;
}

/// Collect the distinct "step" values visible through list() across the whole database.
inline std::set<std::string> list_steps(fdb5::FDB& fdb) {
    const auto base = make_base_key();
    fdb5::FDBToolRequest tool_request(base.request("list"));
    auto iter = fdb.list(tool_request, fdb5::ListMode::Deduplicate);
    fdb5::ListElement elem;
    std::set<std::string> steps;
    while (iter.next(elem)) {
        steps.insert(elem.combinedKey().get("step"));
    }
    return steps;
}

/// Collect the distinct "step" values reported by axes() across the whole database.
inline std::set<std::string> axes_steps(fdb5::FDB& fdb) {
    const auto base = make_base_key();
    fdb5::FDBToolRequest tool_request(base.request());
    auto axis = fdb.axes(tool_request, 3);
    std::set<std::string> steps;
    if (axis.has("step")) {
        for (const auto& value : axis.values("step")) {
            steps.insert(value);
        }
    }
    return steps;
}

/// Archive the complete dataset (all workers' slices) from a single process, then flush.
inline void archive_all(int count) {
    fdb5::FDB fdb;
    for (int worker = 0; worker < count; ++worker) {
        for (int seq = 0; seq < k_seq_per_worker; ++seq) {
            const auto key = make_key(worker, seq);
            const auto data = make_data(worker, seq);
            fdb.archive(key, static_cast<const void*>(data.data()), data.size());
        }
    }
    fdb.flush();
}

//----------------------------------------------------------------------------------------------------------------------
// Worker entry points (run in a freshly exec-ed child process; must not use EXPECT macros).
//
// Each worker returns 0 on success and non-zero on failure.
// fork_and_exec() propagates a non-zero exit as an overall failure that the parent asserts on.

/// Report a failed worker check and return failure from the enclosing function.
#define WORKER_CHECK(cond)                                                                              \
    do {                                                                                                \
        if (!(cond)) {                                                                                  \
            std::cerr << "[worker] check failed: " #cond " at " << __FILE__ << ":" << __LINE__ << '\n'; \
            return 1;                                                                                   \
        }                                                                                               \
    } while (0)

/// Archive this worker's disjoint slice, flush, then self-verify by reading it back.
inline int worker_archive(int worker_id, int /*count*/) {
    fdb5::FDB fdb;
    for (int seq = 0; seq < k_seq_per_worker; ++seq) {
        const auto key = make_key(worker_id, seq);
        const auto data = make_data(worker_id, seq);
        fdb.archive(key, static_cast<const void*>(data.data()), data.size());
    }
    fdb.flush();

    for (int seq = 0; seq < k_seq_per_worker; ++seq) {
        WORKER_CHECK(retrieve_equals(fdb, make_key(worker_id, seq), make_data(worker_id, seq)));
    }
    return 0;
}

/// Retrieve this worker's slice from the pre-archived dataset and verify the bytes.
inline int worker_retrieve(int worker_id, int /*count*/) {
    fdb5::FDB fdb;
    for (int seq = 0; seq < k_seq_per_worker; ++seq) {
        WORKER_CHECK(retrieve_equals(fdb, make_key(worker_id, seq), make_data(worker_id, seq)));
    }
    return 0;
}

/// Inspect each field of this worker's slice; each fully specified request must match exactly one.
inline int worker_inspect(int worker_id, int /*count*/) {
    fdb5::FDB fdb;
    for (int seq = 0; seq < k_seq_per_worker; ++seq) {
        const auto key = make_key(worker_id, seq);
        WORKER_CHECK(inspect_count(fdb, key.request()) == 1);
    }
    return 0;
}

/// List each field of this worker's slice; each fully specified request must match exactly one.
inline int worker_list(int worker_id, int /*count*/) {
    fdb5::FDB fdb;
    for (int seq = 0; seq < k_seq_per_worker; ++seq) {
        const auto key = make_key(worker_id, seq);
        WORKER_CHECK(list_count(fdb, key.request("list"), fdb5::ListMode::Deduplicate) == 1);
    }
    return 0;
}

/// Compute axes over the whole (pre-archived) database and verify the full step set is visible.
inline int worker_axes(int /*worker_id*/, int count) {
    fdb5::FDB fdb;
    WORKER_CHECK(axes_steps(fdb) == expected_steps(count));
    return 0;
}

/// Archive the single shared contention field and flush (all workers write the same key/data).
inline int worker_contend(int /*worker_id*/, int /*count*/) {
    fdb5::FDB fdb;
    const auto key = contend_key();
    const auto data = contend_data();
    fdb.archive(key, static_cast<const void*>(data.data()), data.size());
    fdb.flush();
    return 0;
}

//----------------------------------------------------------------------------------------------------------------------

/// Dispatch a freshly exec-ed child process to the requested worker function.
inline int child_worker_main(int argc, char** argv) {
    const auto args = parse_worker_args(argc, argv);
    const int worker_id = std::stoi(get_worker_arg(args, "worker-id"));
    const auto func = get_worker_arg(args, "fn");
    const int count = env_int("TEST_FDB_WORKER_COUNT", process_count());

    if (func == "archive") {
        return worker_archive(worker_id, count);
    }
    if (func == "retrieve") {
        return worker_retrieve(worker_id, count);
    }
    if (func == "inspect") {
        return worker_inspect(worker_id, count);
    }
    if (func == "list") {
        return worker_list(worker_id, count);
    }
    if (func == "axes") {
        return worker_axes(worker_id, count);
    }
    if (func == "contend") {
        return worker_contend(worker_id, count);
    }

    std::cerr << "[worker] unknown fn: '" << func << "'" << '\n';
    return 1;  // unknown worker
}

//----------------------------------------------------------------------------------------------------------------------

/// creates an isolated FDB root and exports it via FDB5_CONFIG for this process and any children it forks.
/// Also publishes the worker count so exec-ed children can reconstruct the expected result set.

class TestFixture {
public:

    explicit TestFixture(int count) :
        config_{"FDB5_CONFIG", make_config_yaml(root_.asString())},
        workers_{"TEST_FDB_WORKER_COUNT", std::to_string(count)} {}

    TestFixture(const TestFixture&) = delete;
    TestFixture(TestFixture&&) = delete;
    TestFixture& operator=(const TestFixture&) = delete;
    TestFixture& operator=(TestFixture&&) = delete;

    ~TestFixture() = default;

    const eckit::PathName& root() const { return root_; }

private:

    eckit::TmpDir root_;
    eckit::testing::SetEnv config_;
    eckit::testing::SetEnv workers_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb::test::concurrent
