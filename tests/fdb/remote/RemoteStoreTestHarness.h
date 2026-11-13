// SPDX-FileCopyrightText: 2026- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

// Helpers shared by the tests that drive a RemoteStore directly. The environment for these tests is configured by
// an external script, see test_server.sh.in

#pragma once

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Key.h"
#include "fdb5/remote/client/ReadLimiter.h"
#include "fdb5/remote/client/RemoteStore.h"

#include "eckit/exception/Exceptions.h"

#include <memory>
#include <mutex>
#include <string>
#include <utility>

namespace fdb5::remote {

// Friend of RemoteStore (see RemoteStore.h): registers a pending-request queue for a synthetic requestID without
// contacting the server, so tests can drive the store deterministically.
class RemoteStoreTestHarness {
public:

    static std::pair<uint32_t, std::shared_ptr<RemoteStore::MessageQueue>> registerQueue(RemoteStore& store) {
        uint32_t requestID = store.generateRequestID();

        std::lock_guard<std::mutex> lock(store.messageMutex_);
        auto entry = store.messageQueues_.emplace(requestID, std::make_shared<RemoteStore::MessageQueue>(320));
        ASSERT(entry.second);

        return {requestID, entry.first->second};
    }

    static size_t queueCount(RemoteStore& store) {
        std::lock_guard<std::mutex> lock(store.messageMutex_);
        return store.messageQueues_.size();
    }
};

// Friend of ReadLimiter (see ReadLimiter.h)
class ReadLimiterTestHarness {
public:

    static size_t memoryUsed() {
        auto& limiter = ReadLimiter::instance();
        std::lock_guard<std::mutex> lock(limiter.mutex_);
        return limiter.memoryUsed_;
    }

    // Marks a request as sent and holding memory, without contacting a server
    static void markActive(uint32_t clientID, uint32_t requestID, size_t size) {
        auto& limiter = ReadLimiter::instance();
        std::lock_guard<std::mutex> lock(limiter.mutex_);
        limiter.activeRequests_[clientID].insert(requestID);
        limiter.resultSizes_[{clientID, requestID}] = size;
        limiter.memoryUsed_ += size;
    }
};

}  // namespace fdb5::remote

namespace fdb5::test {

// Archives one field and returns the RemoteStore that holds it
inline remote::RemoteStore& storeForOneField() {
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

    std::string data("test data for remote store tests");
    fdb.archive(key, data.data(), data.size());
    fdb.flush();

    auto listIter = fdb.list(FDBToolRequest::requestsFromString("class=od,expver=xxxx,date=20500101")[0]);
    ListElement elem;
    ASSERT(listIter.next(elem));

    return remote::RemoteStore::get(elem.location().uri());
}

}  // namespace fdb5::test
