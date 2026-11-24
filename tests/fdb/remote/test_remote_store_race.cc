// SPDX-FileCopyrightText: 2026- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

// Regression test for a message-ordering hazard between the control and data connections.
//
// RemoteStore::handle() keys pending requests by requestID in messageQueues_. Blob/Complete
// chunks travel on the data connection while Error can now also travel on the control
// connection (see Connection::error()). Because these are two independently-scheduled TCP
// streams, an Error for a given requestID can now be delivered and processed before a Blob
// chunk that was actually sent to the wire earlier on the data connection. This test drives
// RemoteStore::handle() directly, in that adverse order, to prove the client mishandles it.

#include "RemoteStoreTestHarness.h"

#include "fdb5/remote/client/Client.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/io/Buffer.h"
#include "eckit/testing/Test.h"

using namespace eckit::testing;

namespace fdb5::test {

//-----------------------------------------------------------------------------

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
