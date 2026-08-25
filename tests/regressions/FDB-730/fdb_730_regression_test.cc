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
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListElement.h"

#include "eckit/testing/Test.h"

#include <chrono>
#include <cstddef>
#include <exception>
#include <string>
#include <thread>
#include <vector>

//----------------------------------------------------------------------------------------------------------------------

namespace fdb5::test {

namespace {

constexpr size_t lifecycle_iterations = 500;
constexpr size_t lifecycle_threads = 8;

Key test_key() {
    Key key;
    key.set("class", "od");
    key.set("expver", "xxxx");
    key.set("stream", "oper");
    key.set("time", "0000");
    key.set("domain", "g");
    key.set("levtype", "sfc");
    key.set("param", "167");
    key.set("date", "20500101");
    key.set("type", "fc");
    key.set("step", "1");
    return key;
}

size_t archive_test_data() {
    const std::string data = "FDB-730 remote connection lifecycle regression";
    FDB fdb;
    Key key = test_key();
    size_t fields = 0;

    for (const auto& date : {"20500101", "20500102"}) {
        key.set("date", date);
        for (const auto& type : {"fc", "pf"}) {
            key.set("type", type);
            for (const auto& step : {"1", "2"}) {
                key.set("step", step);
                fdb.archive(key, data.data(), data.size());
                ++fields;
            }
        }
    }

    fdb.flush();
    return fields;
}

size_t list_field_count() {
    auto iterator = FDB{}.list(FDBToolRequest{{}, true, {}}, true);
    ListElement element;
    size_t count = 0;
    while (iterator.next(element)) {
        ++count;
    }
    return count;
}

void rethrow_worker_exception(const std::exception_ptr& exception) {
    if (exception) {
        std::rethrow_exception(exception);
    }
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

CASE("FDB-730: concurrent temporary remote clients do not reuse a connection being torn down") {

    constexpr size_t expected_fields = 8;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    EXPECT_EQUAL(archive_test_data(), expected_fields);

    std::vector<std::exception_ptr> exceptions(lifecycle_threads);
    std::vector<size_t> fieldCounts(lifecycle_threads, 0);
    std::vector<std::thread> threads;
    threads.reserve(lifecycle_threads);

    for (size_t threadIndex = 0; threadIndex < lifecycle_threads; ++threadIndex) {
        threads.emplace_back([&, threadIndex] {
            try {
                for (size_t iteration = 0; iteration < lifecycle_iterations; ++iteration) {
                    if (list_field_count() != expected_fields) {
                        fieldCounts[threadIndex] = 0;
                        return;
                    }
                }
                fieldCounts[threadIndex] = expected_fields;
            }
            catch (...) {
                exceptions[threadIndex] = std::current_exception();
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    for (size_t threadIndex = 0; threadIndex < lifecycle_threads; ++threadIndex) {
        rethrow_worker_exception(exceptions[threadIndex]);
        EXPECT_EQUAL(fieldCounts[threadIndex], expected_fields);
    }
}

}  // namespace fdb5::test

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char** argv) {
    return eckit::testing::run_tests(argc, argv);
}
