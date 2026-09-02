/*
 * (C) Copyright 2026- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/AxesIterator.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/api/helpers/StatsIterator.h"
#include "fdb5/api/helpers/WipeIterator.h"

#include "metkit/mars/MarsRequest.h"

#include "eckit/log/Log.h"
#include "eckit/runtime/Main.h"
#include "eckit/testing/ProcessFork.h"
#include "eckit/testing/Test.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <exception>
#include <map>
#include <ostream>
#include <set>
#include <string>
#include <thread>
#include <vector>

namespace fdb5::test {

//----------------------------------------------------------------------------------------------------------------------

// Environment for this test is configured by test_server.sh.in
//
// catalogue and store servers are configured to use single-connection mode
//
// This is a general single-connection concurrency/soak test (no corruption, deadlock, or crash under contention
// on the shared control socket/mutex).
//
// This test does NOT reproduce that hazard: RemoteFDB::forwardApiCall
// registers its message queue before sending the request, and the client only matches
// control-tagged messages against the pending Received promise, so Blob/Complete arriving
// before Received is harmless for List/Inspect/Stats/Axes/Wipe.

namespace {

using StringList = std::vector<std::string>;
using KeyList = std::vector<Key>;

constexpr size_t archive_flush_worker_count = 4;
constexpr size_t archive_flush_batches_per_worker = 24;
constexpr size_t archive_flush_fields_per_batch = 8;
constexpr size_t archive_flush_field_size = static_cast<size_t>(32) * 1024;

Key key_common() {
    Key k;
    k.set("class", "od");
    k.set("expver", "xxxx");
    k.set("stream", "oper");
    k.set("time", "0000");
    k.set("domain", "g");
    k.set("levtype", "sfc");
    k.set("param", "167");
    return k;
}

Key archive_flush_key(size_t worker, size_t batch, size_t field) {
    auto key = key_common();
    key.set("date", "20000101");
    key.set("type", "fc");
    const size_t sequence =
        (((worker * archive_flush_batches_per_worker) + batch) * archive_flush_fields_per_batch) + field;
    key.set("step", std::to_string(sequence + 1));
    return key;
}

KeyList archive_flush_keys() {
    KeyList keys;
    keys.reserve(archive_flush_worker_count * archive_flush_batches_per_worker * archive_flush_fields_per_batch);
    for (size_t worker = 0; worker < archive_flush_worker_count; ++worker) {
        for (size_t batch = 0; batch < archive_flush_batches_per_worker; ++batch) {
            for (size_t field = 0; field < archive_flush_fields_per_batch; ++field) {
                keys.push_back(archive_flush_key(worker, batch, field));
            }
        }
    }
    return keys;
}

int archive_flush_worker(size_t worker) {
    try {
        {
            FDB fdb{};
            const std::string data(archive_flush_field_size, static_cast<char>('a' + worker));

            for (size_t batch = 0; batch < archive_flush_batches_per_worker; ++batch) {
                for (size_t field = 0; field < archive_flush_fields_per_batch; ++field) {
                    const auto key = archive_flush_key(worker, batch, field);
                    fdb.archive(key, data.data(), data.size());
                }
                eckit::Log::info() << "[CLIENT][archive-flush worker " << worker << "] flushing batch " << batch
                                   << '\n';
                fdb.flush();
                eckit::Log::info() << "[CLIENT][archive-flush worker " << worker << "] flushed batch " << batch << '\n';
            }
        }
        eckit::Log::info() << "[CLIENT][archive-flush worker " << worker << "] FDB destroyed" << '\n';
        return 0;
    }
    catch (const std::exception& e) {
        eckit::Log::error() << "[CLIENT][archive-flush worker " << worker << "] " << e.what() << '\n';
        return 1;
    }
}

KeyList write_data(FDB& fdb, const std::string& data, const StringList& dates, const StringList& types,
                   const StringList& steps) {
    KeyList keys;
    auto key = key_common();
    for (const auto& date : dates) {
        key.set("date", date);
        for (const auto& type : types) {
            key.set("type", type);
            for (const auto& step : steps) {
                key.set("step", step);
                fdb.archive(key, data.data(), data.size());
                keys.push_back(key);
            }
        }
    }
    fdb.flush();
    return keys;
}

metkit::mars::MarsRequest make_request(const KeyList& keys) {
    metkit::mars::MarsRequest req;
    for (const auto& [key, value] : keys[0]) {
        req.setValue(key, value);
    }

    std::set<std::string> dates;
    std::set<std::string> types;
    std::set<std::string> steps;
    for (const auto& k : keys) {
        dates.insert(k.get("date"));
        types.insert(k.get("type"));
        steps.insert(k.get("step"));
    }

    req.values("date", StringList(dates.begin(), dates.end()));
    req.values("type", StringList(types.begin(), types.end()));
    req.values("step", StringList(steps.begin(), steps.end()));

    return req;
}

//----------------------------------------------------------------------------------------------------------------------

template <typename Element, typename Iterator>
size_t count_elements(Iterator& iterator) {
    Element elem;
    size_t count = 0;
    while (iterator.next(elem)) {
        ++count;
    }
    return count;
}

bool check_exact_count(size_t count, size_t expected, size_t thread_index, const char* operation) {
    if (count != expected) {
        eckit::Log::error() << "[CLIENT][thread " << thread_index << "] " << operation << ": expected " << expected
                            << ", got " << count << std::endl;
        return false;
    }
    return true;
}

bool check_non_empty(size_t count, size_t thread_index, const char* operation) {
    if (count == 0) {
        eckit::Log::error() << "[CLIENT][thread " << thread_index << "] " << operation
                            << ": expected at least one result" << std::endl;
        return false;
    }
    return true;
}

/// Read an integer environment variable, falling back to @p fallback if unset/invalid.
int env_int(const char* name, int fallback) {
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

int thread_count() {
    return std::max(1, env_int("TEST_FDB_THREAD_COUNT", 8));
}

int iteration_count() {
    return std::max(1, env_int("TEST_FDB_STRESS_ITERATIONS", 20));
}


// Server-side subtoc consolidation happens when a writer's DB session closes, which is triggered by a
// fire-and-forget "Stop" message, so it can race with a subsequent dry-run wipe.
// retry while the catalogue still reports un-consolidated ("unexpected"/UNKNOWN) entries, up to a bounded timeout.
std::map<WipeElementType, size_t> wipe_dry_run_stable(const std::string& request) {

    const auto wait = std::chrono::milliseconds(250);

    std::map<WipeElementType, size_t> element_counts;
    for (size_t attempt = 0; attempt < 80; ++attempt) {
        element_counts.clear();

        auto wipe = FDB{}.wipe(FDBToolRequest::requestsFromString(request)[0], false);
        WipeElement wipe_elem;
        while (wipe.next(wipe_elem)) {
            eckit::Log::info() << "[CLIENT]" << wipe_elem;
            element_counts[wipe_elem.type()] += wipe_elem.uris().size();
        }

        if (element_counts[WipeElementType::UNKNOWN] == 0) {
            return element_counts;
        }
        std::this_thread::sleep_for(wait);
    }
    eckit::Log::error() << "[CLIENT] Failed to reach stable state after 80 attempts.\n";
    return element_counts;
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

CASE("Remote protocol (single connection): concurrent List/Inspect/Stats/Axes/Wipe RPCs are not corrupted") {

    const size_t n_fields = 8;
    const std::string data_string = "Single-connection concurrent RPCs should not corrupt responses.";
    KeyList keys;
    {
        FDB fdb{};  // Expects the config to be set in the environment
        keys = write_data(fdb, data_string, {"20000101", "20000102"}, {"fc", "pf"}, {"1", "2"});
    }
    EXPECT_EQUAL(keys.size(), n_fields);

    const auto request = make_request(keys);
    const auto tool_request = FDBToolRequest{request};
    const size_t n_iterations = iteration_count();

    const size_t n_threads = thread_count();
    constexpr int axes_depth = 3;

    std::vector<int> results(n_threads, -1);
    std::vector<std::thread> threads;
    threads.reserve(n_threads);

    for (size_t thread_index = 0; thread_index < n_threads; ++thread_index) {
        threads.emplace_back([thread_index, n_iterations, &request, &tool_request, &results]() {
            try {
                int result = 0;
                for (size_t i = 0; i < n_iterations && result == 0; ++i) {

                    auto list = FDB{}.list(FDBToolRequest{request}, true);
                    if (!check_exact_count(count_elements<ListElement>(list), n_fields, thread_index, "list")) {
                        result = 1;
                    }

                    auto inspect = FDB{}.inspect(request);
                    if (!check_exact_count(count_elements<ListElement>(inspect), n_fields, thread_index, "inspect")) {
                        result = 1;
                    }

                    auto stats = FDB{}.stats(tool_request);
                    if (!check_non_empty(count_elements<StatsElement>(stats), thread_index, "stats")) {
                        result = 1;
                    }

                    auto axes = FDB{}.axesIterator(tool_request, axes_depth);
                    if (!check_non_empty(count_elements<AxesElement>(axes), thread_index, "axes")) {
                        result = 1;
                    }

                    // dry-run only: concurrent doit=true wipes would race on deleting shared data
                    auto wipe = FDB{}.wipe(tool_request, false);
                    if (!check_non_empty(count_elements<WipeElement>(wipe), thread_index, "wipe (dry-run)")) {
                        result = 1;
                    }
                }
                results[thread_index] = result;
            }
            catch (const std::exception& e) {
                eckit::Log::error() << "[CLIENT][thread " << thread_index << "] " << e.what() << std::endl;
                results[thread_index] = 1;
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    for (auto result : results) {
        EXPECT_EQUAL(result, 0);
    }

    // wait for the reconsolidation to finish. Checking with dry-run wipe(s)
    wipe_dry_run_stable("class=od");

    // Clean up
    eckit::Log::info() << "[CLIENT]" << "Wiping single-connection stress test data. --doit" << std::endl;
    auto wipeit = FDB{}.wipe(FDBToolRequest::requestsFromString("class=od")[0], true);
    WipeElement wipe_elem;
    while (wipeit.next(wipe_elem)) {
        eckit::Log::info() << "[CLIENT]" << wipe_elem;
    }
}

CASE("Remote protocol (single connection): concurrent clients archive and flush are ordered") {

    EXPECT(eckit::testing::fork_and_exec(static_cast<int>(archive_flush_worker_count), {"--fn=archive-flush"}));

    const auto keys = archive_flush_keys();
    FDB fdb{};
    auto list = fdb.list(FDBToolRequest{make_request(keys)}, true);
    EXPECT_EQUAL(count_elements<ListElement>(list), keys.size());

    wipe_dry_run_stable("class=od");

    eckit::Log::info() << "[CLIENT] Wiping concurrent archive and flush test data. --doit" << std::endl;
    auto wipeit = FDB{}.wipe(FDBToolRequest::requestsFromString("class=od")[0], true);
    WipeElement wipe_elem;
    while (wipeit.next(wipe_elem)) {
        eckit::Log::info() << "[CLIENT]" << wipe_elem;
    }
}

}  // namespace fdb5::test

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char** argv) {
    const auto args = eckit::testing::parse_worker_args(argc, argv);
    if (!args.empty()) {
        eckit::Main::initialise(argc, argv);
        if (eckit::testing::get_worker_arg(args, "fn") == "archive-flush") {
            return fdb5::test::archive_flush_worker(std::stoul(eckit::testing::get_worker_arg(args, "worker-id")));
        }
        return 1;
    }
    return eckit::testing::run_tests(argc, argv);
}
