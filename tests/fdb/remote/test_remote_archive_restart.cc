// SPDX-FileCopyrightText: 2026- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

// Archiving through a long-lived FDB handle must survive a store restart: a flush() waiting for fields the store
// never stored must fail instead of blocking, and later archives on the same handle must work again.

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/database/Key.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/testing/Test.h"

#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <thread>

using namespace eckit::testing;

namespace fdb5::test {

namespace {

Key fieldKey(int step) {
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
    key.set("step", std::to_string(step));
    return key;
}

std::string fieldData(int step) {
    return "field for step " + std::to_string(step);
}

void archive(FDB& fdb, int step) {
    const std::string data = fieldData(step);
    fdb.archive(fieldKey(step), data.data(), data.size());
}

// True if the catalogue lists the field, i.e. it was archived and flushed. Reading the data back is not checked here:
// read stores do not reconnect after a store restart (see #421).
bool listed(int step) {
    FDB fdb{};
    auto request =
        FDBToolRequest::requestsFromString("class=od,expver=xxxx,date=20500101,step=" + std::to_string(step))[0];
    auto listIter = fdb.list(request);
    ListElement elem;
    return listIter.next(elem);
}

// Runs op in a detached thread and returns its error ("" if none), so that an op that never returns fails the test
// instead of hanging it
std::future<std::string> start(std::function<void()> op) {
    auto result = std::make_shared<std::promise<std::string>>();
    auto future = result->get_future();
    std::thread([op, result] {
        try {
            op();
            result->set_value("");
        }
        catch (const std::exception& e) {
            result->set_value(e.what());
        }
    }).detach();
    return future;
}

void signalStore(long pgid, int signal) {
    ASSERT(pgid > 0);
    ASSERT(::kill(-pgid, signal) == 0);
}

// Restarts the store and returns the process group of the new server
long restartStore() {
    const char* script = std::getenv("FDB_REMOTE_STORE_RESTART");
    ASSERT(script != nullptr);
    FILE* out = ::popen(script, "r");
    ASSERT(out != nullptr);
    // Read up to the line with the process group: do not wait for the end of the output, which the restarted
    // server's background processes keep open
    char line[64] = {};
    long pgid     = 0;
    while (pgid <= 0 && std::fgets(line, sizeof(line), out) != nullptr) {
        pgid = std::atol(line);
    }
    ASSERT(::pclose(out) == 0);
    ASSERT(pgid > 0);
    return pgid;
}

}  // namespace

//-----------------------------------------------------------------------------
// Run by test_server.sh.in, which exports the pgids of the servers and the scripts that restart them

CASE("Remote archive: a long-lived handle survives store restarts") {

    FDB fdb{};
    long storePgid = std::atol(std::getenv("FDB_REMOTE_STORE_PGID"));

    // Opens the database: catalogue and store connections are cached in the handle
    archive(fdb, 1);
    fdb.flush();
    EXPECT(listed(1));

    {
        // A flush() waiting for a field the store never stored fails when the store is lost
        archive(fdb, 2);  // starts a cycle, the store stores it

        // The store does not store the next field: its location never arrives
        signalStore(storePgid, SIGSTOP);
        archive(fdb, 3);
        auto flushed = start([&fdb] { fdb.flush(); });
        EXPECT(flushed.wait_for(std::chrono::seconds(1)) == std::future_status::timeout);  // waiting for step 3

        signalStore(storePgid, SIGKILL);
        EXPECT(flushed.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
        if (flushed.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
            const std::string error = flushed.get();
            EXPECT(error.find("archived field(s) were stored") != std::string::npos);
        }

        storePgid = restartStore();

        // The same handle opens the database again
        auto archived = start([&fdb] {
            archive(fdb, 4);
            fdb.flush();
        });
        EXPECT(archived.wait_for(std::chrono::seconds(20)) == std::future_status::ready);
        if (archived.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
            EXPECT_EQUAL(archived.get(), "");
        }
        EXPECT(listed(4));
        EXPECT(!listed(3));  // reported as lost by the failed flush()
    }

    {
        // A handle that was idle while the store restarted archives without an error
        signalStore(storePgid, SIGKILL);
        storePgid = restartStore();

        auto archived = start([&fdb] {
            archive(fdb, 5);
            fdb.flush();
        });
        EXPECT(archived.wait_for(std::chrono::seconds(20)) == std::future_status::ready);
        if (archived.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
            EXPECT_EQUAL(archived.get(), "");
        }
        EXPECT(listed(5));
    }
}

}  // namespace fdb5::test

//-----------------------------------------------------------------------------

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
