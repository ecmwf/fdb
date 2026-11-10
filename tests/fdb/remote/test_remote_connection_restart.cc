// SPDX-FileCopyrightText: 2026- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

// A request waiting on a server should get an error when the connection is lost, also if the server comes back up
// afterwards: the request must neither be resumed on a new connection nor stay blocked.

#include "RemoteFDBTestHarness.h"
#include "RemoteStoreTestHarness.h"

#include "fdb5/config/Config.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/io/Buffer.h"
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

namespace {

// Blocks a reader in queue->pop() and returns what it got. Detached, so that a reader that never wakes up fails the
// test instead of hanging it.
template <typename Message, typename Queue>
std::future<std::string> startReader(std::shared_ptr<Queue> queue) {
    auto result  = std::make_shared<std::promise<std::string>>();
    auto future  = result->get_future();
    auto started = std::make_shared<std::promise<void>>();

    std::thread([queue, result, started] {
        Message msg;
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

    return future;
}

void killServer(const char* pgidVariable) {
    const char* pgid = std::getenv(pgidVariable);
    ASSERT(pgid != nullptr);
    const long pgidValue = std::atol(pgid);
    ASSERT(pgidValue > 0);

    ASSERT(::kill(-pgidValue, SIGKILL) == 0);
}

void restartServer(const char* scriptVariable) {
    const char* script = std::getenv(scriptVariable);
    ASSERT(script != nullptr);

    ASSERT(std::system(script) == 0);
}

void expectWokenWith(std::future<std::string>& reader, const std::string& message) {
    EXPECT(reader.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
    if (reader.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
        EXPECT(reader.get().find(message) != std::string::npos);
    }
}

}  // namespace

//-----------------------------------------------------------------------------
// Run by test_server.sh.in, which exports the pgids of the servers and the scripts that restart them

CASE("Remote store: a request that is waiting is woken up, not resumed, when the store is restarted") {

    remote::RemoteStore& store = storeForOneField();
    auto queue                 = remote::RemoteStoreTestHarness::registerQueue(store).second;

    auto reader = startReader<remote::RemoteStore::StoredMessage>(queue);

    killServer("FDB_REMOTE_STORE_PGID");
    restartServer("FDB_REMOTE_STORE_RESTART");

    expectWokenWith(reader, "Unexpected closure of store");
}

CASE("Remote FDB: a request that is waiting is woken up, not resumed, when the catalogue is restarted") {

    Config config = Config{}.expandConfig();
    RemoteFDB fdb(config, "remote");
    auto queue = RemoteFDBTestHarness::registerQueue(fdb);

    auto reader = startReader<eckit::Buffer>(queue);

    killServer("FDB_REMOTE_CATALOGUE_PGID");
    restartServer("FDB_REMOTE_CATALOGUE_RESTART");

    expectWokenWith(reader, "Unexpected closure of remote FDB");
}

}  // namespace fdb5::test

//-----------------------------------------------------------------------------

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
