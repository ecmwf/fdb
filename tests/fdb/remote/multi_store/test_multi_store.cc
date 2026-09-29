/*
 * (C) Copyright 2026- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

/// Reproducer for the concurrent-retrieve race on a local catalogue backed by two remote stores.
///
/// The environment (two fdb-server store processes + a local-catalogue/remote-store client config)
/// is configured by an external script. See multi_store.sh.in.
///
/// Archiving spans several databases (one per date); each database's store is bound to a randomly
/// selected store endpoint, so fields end up spread across both stores. Many threads then retrieve
/// every field concurrently through the process-wide shared RemoteStore/ClientConnection, which is
/// what triggers the blocking-RPC/promise race.

#include "fdb5/api/FDB.h"
#include "fdb5/database/Key.h"

#include "eckit/io/DataHandle.h"
#include "eckit/log/Log.h"
#include "eckit/testing/Test.h"

#include <array>
#include <cstddef>
#include <exception>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace fdb5::test {

//-----------------------------------------------------------------------------

namespace {

constexpr int k_dates = 8;
constexpr int k_params = 4;
constexpr int k_steps = 4;

constexpr size_t k_threads = 16;
constexpr int k_iterations = 8;

std::string date_of(int index) {
    return std::to_string(20250101 + index);
}

Key make_key(int date, int param, int step) {
    Key key;
    key.set("class", "od");
    key.set("expver", "xxxx");
    key.set("stream", "oper");
    key.set("date", date_of(date));
    key.set("time", "0000");
    key.set("domain", "g");
    key.set("type", "fc");
    key.set("levtype", "sfc");
    key.set("step", std::to_string(step));
    key.set("param", std::to_string(130 + param));
    return key;
}

std::string make_data(int date, int param, int step) {
    return "d=" + date_of(date) + ";p=" + std::to_string(130 + param) + ";s=" + std::to_string(step);
}

std::string read_all(eckit::DataHandle& handle) {
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

}  // namespace

//-----------------------------------------------------------------------------

CASE("Remote FDB: Concurrent retrieve across two remote stores") {

    {
        FDB fdb;
        for (int date = 0; date < k_dates; ++date) {
            for (int param = 0; param < k_params; ++param) {
                for (int step = 0; step < k_steps; ++step) {
                    const auto key = make_key(date, param, step);
                    const auto data = make_data(date, param, step);
                    fdb.archive(key, data.data(), data.size());
                }
            }
        }
        fdb.flush();
    }

    struct Field {
        metkit::mars::MarsRequest request;
        std::string bytes;
    };
    std::vector<Field> fields;
    fields.reserve(static_cast<size_t>(k_dates) * k_params * k_steps);
    for (int date = 0; date < k_dates; ++date) {
        for (int param = 0; param < k_params; ++param) {
            for (int step = 0; step < k_steps; ++step) {
                fields.push_back({make_key(date, param, step).request("retrieve"), make_data(date, param, step)});
            }
        }
    }

    // Sanity check
    {
        FDB fdb;
        for (const auto& field : fields) {
            std::unique_ptr<eckit::DataHandle> handle(fdb.retrieve(field.request));
            EXPECT(handle != nullptr);
            EXPECT_EQUAL(read_all(*handle), field.bytes);
        }
    }

    std::vector<int> results(k_threads, -1);
    std::vector<std::thread> threads;
    threads.reserve(k_threads);
    for (size_t thread = 0; thread < k_threads; ++thread) {
        threads.emplace_back([thread, &fields, &results]() {
            try {
                int result = 0;
                for (int round = 0; round < k_iterations && result == 0; ++round) {
                    FDB fdb;
                    for (const auto& field : fields) {
                        std::unique_ptr<eckit::DataHandle> handle(fdb.retrieve(field.request));
                        if (!handle || read_all(*handle) != field.bytes) {
                            result = 1;
                            break;
                        }
                    }
                }
                results[thread] = result;
            }
            catch (const std::exception& error) {
                eckit::Log::error() << "[thread " << thread << "] " << error.what() << std::endl;
                results[thread] = 1;
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    for (auto result : results) {
        EXPECT_EQUAL(result, 0);
    }
}

//-----------------------------------------------------------------------------

}  // namespace fdb5::test

int main(int argc, char** argv) {
    return eckit::testing::run_tests(argc, argv);
}
