// SPDX-FileCopyrightText: 2026- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

// A request waiting on a server should get an error when the connection is lost, also if the server comes back up
// afterwards: the request must neither be resumed on a new connection nor stay blocked.
// Once the server is back, later requests in the same process must work again.

#include "RemoteFDBTestHarness.h"
#include "RemoteStoreTestHarness.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/config/Config.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/io/Buffer.h"
#include "eckit/io/DataHandle.h"
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

// Reads the field archived by storeForOneField() the way a retrieval does: RemoteFieldLocation::dataHandle(), which
// goes through RemoteStore::get(). Returns the data, or the error.
std::string readTestField() {
    try {
        FDB fdb{};
        auto listIter = fdb.list(FDBToolRequest::requestsFromString("class=od,expver=xxxx,date=20500101")[0]);
        ListElement elem;
        ASSERT(listIter.next(elem));

        std::unique_ptr<eckit::DataHandle> dh(elem.location().dataHandle());
        dh->openForRead();
        std::string data;
        char buf[1024];
        long n = 0;
        while ((n = dh->read(buf, sizeof(buf))) > 0) {
            data.append(buf, n);
        }
        dh->close();
        return data;
    }
    catch (const std::exception& e) {
        return std::string("error: ") + e.what();
    }
}

// The client may only notice that the connection is gone on the first request after the restart, so allow a few
// failed attempts before the data must come back.
std::string readTestFieldWithRetries(const std::string& expected) {
    std::string result;
    for (int attempt = 0; attempt < 10; ++attempt) {
        result = readTestField();
        if (result == expected) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return result;
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

    const std::string expected = "test data for remote store tests";

    remote::RemoteStore& store = storeForOneField();
    auto queue                 = remote::RemoteStoreTestHarness::registerQueue(store).second;
    EXPECT_EQUAL(readTestField(), expected);

    auto reader = startReader<remote::RemoteStore::StoredMessage>(queue);

    killServer("FDB_REMOTE_STORE_PGID");
    restartServer("FDB_REMOTE_STORE_RESTART");

    expectWokenWith(reader, "Unexpected closure of store");

    // The store's connection is now lost, but it is still the cached store: nothing has called get() since
    FDB fdb{};
    auto listIter = fdb.list(FDBToolRequest::requestsFromString("class=od,expver=xxxx,date=20500101")[0]);
    ListElement elem;
    ASSERT(listIter.next(elem));
    const auto& location = elem.location();

    const size_t memoryUsed = remote::ReadLimiterTestHarness::memoryUsed();
    const size_t queues     = remote::RemoteStoreTestHarness::queueCount(store);

    // A new read on it, e.g. by a thread that got it from get() just before another thread replaced it, fails
    // fast and leaves nothing behind
    try {
        std::unique_ptr<eckit::DataHandle> dh(store.dataHandle(location));
        EXPECT(false);  // must have thrown
    }
    catch (const remote::RemoteFDBException& e) {
        EXPECT(std::string(e.what()).find("is no longer valid") != std::string::npos);
    }
    EXPECT_EQUAL(remote::RemoteStoreTestHarness::queueCount(store), queues);
    EXPECT_EQUAL(remote::ReadLimiterTestHarness::memoryUsed(), memoryUsed);

    // A read whose send fails in the ReadLimiter gives its memory back, and its reader gets the error
    auto [failedID, failedQueue] = remote::RemoteStoreTestHarness::registerQueue(store);
    EXPECT_NO_THROW(remote::ReadLimiter::instance().add(&store, failedID, location, Key{}));
    EXPECT_EQUAL(remote::ReadLimiterTestHarness::memoryUsed(), memoryUsed);
    EXPECT_EQUAL(remote::RemoteStoreTestHarness::queueCount(store), queues);
    auto failedReader = startReader<remote::RemoteStore::StoredMessage>(failedQueue);
    expectWokenWith(failedReader, "is no longer valid");

    // The store read from above lost its connection: later reads in this process must work again
    EXPECT_EQUAL(readTestFieldWithRetries(expected), expected);
}

CASE("ReadLimiter: finishing a request that was never sent does not touch the others") {
    // e.g. the DataHandle of a queued request is destroyed after its reader was woken by a lost connection, while
    // the same store has another request in flight
    const uint32_t clientID   = 999999;
    const size_t memoryBefore = remote::ReadLimiterTestHarness::memoryUsed();
    remote::ReadLimiterTestHarness::markActive(clientID, 1, 100);

    EXPECT_NO_THROW(remote::ReadLimiter::instance().finishRequest(clientID, 2));
    EXPECT_EQUAL(remote::ReadLimiterTestHarness::memoryUsed(), memoryBefore + 100);

    remote::ReadLimiter::instance().finishRequest(clientID, 1);
    EXPECT_EQUAL(remote::ReadLimiterTestHarness::memoryUsed(), memoryBefore);
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
