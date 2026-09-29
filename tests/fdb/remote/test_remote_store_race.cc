/*
 * (C) Copyright 2026- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

// Regression test for a message-ordering hazard between the control and data connections.
//
// RemoteStore::handle() keys pending requests by requestID in messageQueues_. Blob/Complete
// chunks travel on the data connection while Error can now also travel on the control
// connection (see Connection::error()). Because these are two independently-scheduled TCP
// streams, an Error for a given requestID can now be delivered and processed before a Blob
// chunk that was actually sent to the wire earlier on the data connection. This test drives
// RemoteStore::handle() directly, in that adverse order, to prove the client mishandles it.

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Key.h"
#include "fdb5/remote/client/Client.h"
#include "fdb5/remote/client/RemoteStore.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/io/Buffer.h"
#include "eckit/testing/Test.h"

#include <memory>
#include <mutex>
#include <utility>

using namespace eckit::testing;

namespace fdb5::remote {

// Friend of RemoteStore (see RemoteStore.h): registers a pending-request queue for a synthetic
// requestID without contacting the server, so tests can drive handle() deterministically.
class RemoteStoreTestHarness {
public:

    static std::pair<uint32_t, std::shared_ptr<RemoteStore::MessageQueue>> registerQueue(RemoteStore& store) {
        uint32_t requestID = store.generateRequestID();

        std::lock_guard<std::mutex> lock(store.messageMutex_);
        auto entry = store.messageQueues_.emplace(requestID, std::make_shared<RemoteStore::MessageQueue>(320));
        ASSERT(entry.second);

        return {requestID, entry.first->second};
    }
};

}  // namespace fdb5::remote

namespace fdb5::test {

//-----------------------------------------------------------------------------
// Note: the environment for this test is configured by an external script. See tests/remote/test_server.sh.in

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

    std::string data("test data for remote store race test");
    fdb.archive(key, data.data(), data.size());
    fdb.flush();

    auto listIter = fdb.list(FDBToolRequest::requestsFromString("class=od,expver=xxxx,date=20500101")[0]);
    ListElement elem;
    ASSERT(listIter.next(elem));

    return remote::RemoteStore::get(elem.location().uri());
}

CASE("Remote store: well-ordered messages are handled cleanly") {

    remote::RemoteStore& store = storeForOneField();
    auto [requestID, queue] = remote::RemoteStoreTestHarness::registerQueue(store);

    remote::Client& client = store;

    // Blob delivered on the data connection, followed by Complete: the normal, well-ordered case.
    EXPECT(client.handle(remote::Message::Blob, requestID, eckit::Buffer{"chunk", 5}));
    EXPECT(client.handle(remote::Message::Complete, requestID));

    remote::RemoteStore::StoredMessage msg;
    EXPECT(queue->pop(msg) != -1);
    EXPECT(msg.first == remote::Message::Blob);
    EXPECT(queue->pop(msg) != -1);
    EXPECT(msg.first == remote::Message::Complete);
}

CASE("Remote store: Error arriving before a still-in-flight Blob crashes the client") {

    remote::RemoteStore& store = storeForOneField();
    auto [requestID, queue] = remote::RemoteStoreTestHarness::registerQueue(store);

    remote::Client& client = store;

    // the control connection sends Error before the data connection delivers a Blob
    EXPECT(client.handle(remote::Message::Error, requestID, eckit::Buffer{"synthetic failure", 18}));

    EXPECT_THROWS_AS(client.handle(remote::Message::Blob, requestID, eckit::Buffer{"late chunk", 10}),
                     eckit::AssertionFailed);
}

}  // namespace fdb5::test

//-----------------------------------------------------------------------------

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
