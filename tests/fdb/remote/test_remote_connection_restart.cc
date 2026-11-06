// SPDX-FileCopyrightText: 2026- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

// A request waiting on the store should get an error when the connection is lost, also if the store comes back up
// afterwards: the request must neither be resumed on a new connection nor stay blocked.

#include "RemoteStoreTestHarness.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/testing/Test.h"

#include <chrono>
#include <csignal>
#include <cstdlib>
#include <future>
#include <memory>
#include <string>
#include <thread>

using namespace eckit::testing;

namespace fdb5::test {

//-----------------------------------------------------------------------------
// Run by test_server.sh.in, which exports FDB_REMOTE_STORE_PGID and FDB_REMOTE_STORE_RESTART

CASE("Remote store: a request that is waiting is woken up, not resumed, when the store is restarted") {

    remote::RemoteStore& store = storeForOneField();
    auto queue                 = remote::RemoteStoreTestHarness::registerQueue(store).second;

    const char* pgid = std::getenv("FDB_REMOTE_STORE_PGID");
    ASSERT(pgid != nullptr);
    const long pgidValue = std::atol(pgid);
    ASSERT(pgidValue > 0);

    const char* restart = std::getenv("FDB_REMOTE_STORE_RESTART");
    ASSERT(restart != nullptr);

    auto result      = std::make_shared<std::promise<std::string>>();
    auto interrupted = result->get_future();
    auto started     = std::make_shared<std::promise<void>>();

    // detached, so that a reader that never wakes up fails the test instead of hanging it
    std::thread([queue, result, started] {
        remote::RemoteStore::StoredMessage msg;
        started->set_value();
        try {
            queue->pop(msg);
            result->set_value("pop() returned");
        }
        catch (const std::exception& e) {
            result->set_value(e.what());
        }
        catch (...) {
            result->set_value("unknown exception");
        }
    }).detach();

    // Give the reader time to block in pop() before the connection is lost
    started->get_future().wait();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    ASSERT(::kill(-pgidValue, SIGKILL) == 0);
    ASSERT(std::system(restart) == 0);

    EXPECT(interrupted.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
    if (interrupted.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
        EXPECT(interrupted.get().find("Unexpected closure of store") != std::string::npos);
    }
}

}  // namespace fdb5::test

//-----------------------------------------------------------------------------

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
