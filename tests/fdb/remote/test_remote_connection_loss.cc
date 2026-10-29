// SPDX-FileCopyrightText: 2026- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

// A request waiting on the store should get an error, not wait forever, when the connection to the store is lost.

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Key.h"
#include "fdb5/remote/client/RemoteStore.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/testing/Test.h"

#include <chrono>
#include <csignal>
#include <cstdlib>
#include <future>
#include <memory>
#include <thread>

using namespace eckit::testing;

namespace fdb5::remote {

// Registers a pending request on the store without contacting the server
class RemoteStoreTestHarness {
public:

    static std::shared_ptr<RemoteStore::MessageQueue> registerQueue(RemoteStore& store) {
        uint32_t requestID = store.generateRequestID();

        std::lock_guard<std::mutex> lock(store.messageMutex_);
        auto entry = store.messageQueues_.emplace(requestID, std::make_shared<RemoteStore::MessageQueue>(320));
        ASSERT(entry.second);

        return entry.first->second;
    }
};

}  // namespace fdb5::remote

namespace fdb5::test {

//-----------------------------------------------------------------------------
// Run by test_server.sh.in, which exports FDB_REMOTE_STORE_PGID

remote::RemoteStore& storeForOneField() {
    FDB fdb{};

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

    std::string data("test data for remote connection loss test");
    fdb.archive(key, data.data(), data.size());
    fdb.flush();

    auto listIter = fdb.list(FDBToolRequest::requestsFromString("class=od,expver=xxxx,date=20500101")[0]);
    ListElement elem;
    ASSERT(listIter.next(elem));

    return remote::RemoteStore::get(elem.location().uri());
}

CASE("Remote store: losing the connection wakes up a request that is waiting") {

    remote::RemoteStore& store = storeForOneField();
    auto queue                 = remote::RemoteStoreTestHarness::registerQueue(store);

    const char* pgid = std::getenv("FDB_REMOTE_STORE_PGID");
    ASSERT(pgid != nullptr);
    ASSERT(::kill(-std::atol(pgid), SIGKILL) == 0);

    auto result      = std::make_shared<std::promise<bool>>();
    auto interrupted = result->get_future();

    // detached, so that a reader that never wakes up fails the test instead of hanging it
    std::thread([queue, result] {
        remote::RemoteStore::StoredMessage msg;
        try {
            queue->pop(msg);
            result->set_value(false);
        }
        catch (const eckit::Exception&) {
            result->set_value(true);
        }
    }).detach();

    EXPECT(interrupted.wait_for(std::chrono::seconds(10)) == std::future_status::ready && interrupted.get());
}

}  // namespace fdb5::test

//-----------------------------------------------------------------------------

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
