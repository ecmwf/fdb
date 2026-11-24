// SPDX-FileCopyrightText: 2026- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

// The client must never mistake a failed archival for a complete one: when the server fails an archive request,
// RemoteStore::flush() has to fail, wherever the failure lands relative to the checks that flush() performs.

#include "fdb5/api/FDB.h"
#include "fdb5/api/FDBFactory.h"
#include "fdb5/api/LocalFDB.h"
#include "fdb5/database/Archiver.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Store.h"
#include "fdb5/remote/client/RemoteStore.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/testing/Test.h"

#include <chrono>
#include <exception>
#include <future>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <vector>

using namespace eckit::testing;

namespace fdb5::test {

//-----------------------------------------------------------------------------

namespace {

std::exception_ptr serverFailure() {
    return std::make_exception_ptr(eckit::SeriousBug("synthetic archive failure"));
}

void archive(remote::Locations& locations, uint32_t requestID) {
    locations.archive(requestID, [](const std::unique_ptr<const FieldLocation>&) {});
}

class FailingFlushFDB : public LocalFDB {
public:

    using LocalFDB::LocalFDB;

    void archive(const Key&, const void*, size_t) override {}
    void flush() override {
        ++flushes;
        throw eckit::SeriousBug("synthetic flush failure");
    }

    static int flushes;
};

int FailingFlushFDB::flushes = 0;

class FailingStore : public Store {
public:

    explicit FailingStore(int& flushes) : flushes_(flushes) {}

    size_t flush() override {
        ++flushes_;
        throw eckit::SeriousBug("synthetic store failure");
    }

    eckit::DataHandle* retrieve(Field&) const override { NOTIMP; }
    void remove(const eckit::URI&, std::ostream&, std::ostream&, bool) const override { NOTIMP; }
    void print(std::ostream&) const override { NOTIMP; }
    std::string type() const override { NOTIMP; }
    bool open() override { NOTIMP; }
    void close() override { NOTIMP; }
    bool exists() const override { NOTIMP; }
    void checkUID() const override { NOTIMP; }
    eckit::URI uri() const override { NOTIMP; }
    bool uriBelongs(const eckit::URI&) const override { NOTIMP; }
    bool uriExists(const eckit::URI&) const override { NOTIMP; }
    std::set<eckit::URI> collocatedDataURIs() const override { NOTIMP; }
    std::set<eckit::URI> asCollocatedDataURIs(const std::set<eckit::URI>&) const override { NOTIMP; }
    std::vector<eckit::URI> getAuxiliaryURIs(const eckit::URI&, bool) const override { NOTIMP; }
    void finaliseWipeState(StoreWipeState&, bool, bool) override { NOTIMP; }
    bool doWipeUnknowns(const std::set<eckit::URI>&) const override { NOTIMP; }
    bool doWipeURIs(const StoreWipeState&) const override { NOTIMP; }
    void doWipeEmptyDatabase() const override { NOTIMP; }
    bool doUnsafeFullWipe() const override { NOTIMP; }

private:

    int& flushes_;
};

class TestArchiver : public Archiver {
public:

    explicit TestArchiver(int& flushes) : Archiver(Config{}) {
        Key first;
        first.set("id", "first");
        Key second;
        second.set("id", "second");
        databases_.emplace(first, Database{0, nullptr, std::make_unique<FailingStore>(flushes)});
        databases_.emplace(second, Database{0, nullptr, std::make_unique<FailingStore>(flushes)});
    }
};

}  // namespace

//-----------------------------------------------------------------------------

CASE("Locations: complete when all the locations have been received") {
    remote::Locations locations;

    archive(locations, 1);
    EXPECT(!locations.finished());
    EXPECT(!locations.complete());

    EXPECT(locations.location(1, nullptr));

    // n.b. forwarded to the catalogue by another thread
    EXPECT(locations.wait() == 1);
    EXPECT(locations.finished());
}

CASE("Locations: a slow catalogue does not block the delivery of the locations") {
    remote::Locations locations;

    // the catalogue takes a while to archive the first location
    std::promise<void> catalogueAnswers;
    std::shared_future<void> answered = catalogueAnswers.get_future().share();
    locations.archive(1, [answered](const std::unique_ptr<const FieldLocation>&) { answered.wait(); });
    archive(locations, 2);
    EXPECT(!locations.complete());

    // the listener of the connection to the store delivers the locations without waiting for the catalogue
    auto delivered = std::async(
        std::launch::async, [&locations] { return locations.location(1, nullptr) && locations.location(2, nullptr); });
    // n.b. declared after the future: the catalogue answers before the future waits, also if the test fails
    struct Answer {
        std::promise<void>& answer;
        bool done = false;
        void now() {
            if (!done) {
                done = true;
                answer.set_value();
            }
        }
        ~Answer() { now(); }
    } answer{catalogueAnswers};

    EXPECT(delivered.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
    EXPECT(!locations.finished());  // not forwarded yet

    answer.now();
    EXPECT(delivered.get());
    EXPECT(locations.wait() == 2);
}

CASE("Locations: a failure of the catalogue fails the archival, not the connection to the store") {
    remote::Locations locations;

    locations.archive(
        1, [](const std::unique_ptr<const FieldLocation>&) { throw eckit::SeriousBug("synthetic catalogue failure"); });
    EXPECT(!locations.complete());

    EXPECT_NO_THROW(locations.location(1, nullptr));  // n.b. the listener of the connection is not affected
    EXPECT_THROWS_AS(locations.wait(), eckit::SeriousBug);
    EXPECT_THROWS_AS(locations.complete(), eckit::SeriousBug);
}

CASE("Locations: a failure that lands before the flush checks is not reported as completion") {
    remote::Locations locations;

    archive(locations, 1);
    archive(locations, 2);

    // the failure arrives between flush() deciding to proceed and checking for completion
    EXPECT(locations.fail(2, serverFailure()));

    EXPECT_THROWS_AS(locations.complete(), eckit::SeriousBug);
    EXPECT_THROWS_AS(archive(locations, 3), eckit::SeriousBug);
    EXPECT(locations.finished());  // nothing more will arrive (so that the destructor can proceed)
}

CASE("Locations: a failure while waiting wakes the waiter up with the failure") {
    remote::Locations locations;

    archive(locations, 1);
    archive(locations, 2);

    EXPECT(!locations.complete());

    auto waiter = std::async(std::launch::async, [&] { return locations.wait(); });

    locations.failPending(serverFailure());
    EXPECT_THROWS_AS(static_cast<void>(waiter.get()), eckit::SeriousBug);
}

CASE("Locations: a failure of one request fails all the pending ones, whoever sent them") {
    remote::Locations locations;

    archive(locations, 1);
    archive(locations, 2);
    archive(locations, 3);

    EXPECT(!locations.complete());
    EXPECT(locations.fail(3, serverFailure()));

    EXPECT_THROWS_AS(locations.wait(), eckit::SeriousBug);
    EXPECT_THROWS_AS(locations.complete(), eckit::SeriousBug);
}

CASE("Locations: a location arriving after a failure doesn't satisfy the promise twice") {
    remote::Locations locations;

    archive(locations, 1);
    EXPECT(!locations.complete());

    EXPECT(locations.fail(1, serverFailure()));

    // all the locations are now in, but the failure has already been reported to the waiter
    EXPECT_NO_THROW(locations.location(1, nullptr));
    EXPECT_THROWS_AS(locations.wait(), eckit::SeriousBug);
}

CASE("Locations: only the requests that are pending can fail the archival") {
    remote::Locations locations;

    archive(locations, 1);
    EXPECT(!locations.fail(42, serverFailure()));
    EXPECT(!locations.complete());

    // nothing pending: a lost connection is not a failure of the archival
    remote::Locations idle;
    idle.failPending(serverFailure());
    EXPECT(idle.complete());
}

CASE("Locations: a lost connection fails the flush while the last location is still being forwarded") {
    // n.b. declared before the Locations: it outlives its forwarding thread, which may still use it when the Locations
    // is destroyed (e.g. if the test fails before the callback has started)
    std::promise<void> forwarding;

    remote::Locations locations;

    // the catalogue does not answer for the last location
    std::promise<void> catalogueAnswers;
    std::shared_future<void> answered = catalogueAnswers.get_future().share();
    locations.archive(1, [answered, &forwarding](const std::unique_ptr<const FieldLocation>&) {
        forwarding.set_value();
        answered.wait();
    });
    EXPECT(!locations.complete());

    auto waiter = std::async(std::launch::async, [&locations] { return locations.wait(); });
    // n.b. declared after the future: the catalogue answers before the future waits, also if the test fails
    struct Answer {
        std::promise<void>& answer;
        bool done = false;
        void now() {
            if (!done) {
                done = true;
                answer.set_value();
            }
        }
        ~Answer() { now(); }
    } answer{catalogueAnswers};

    // the last location is received: nothing is expected from the store any more, but it is being forwarded
    EXPECT(locations.location(1, nullptr));
    auto forwardingStarted = forwarding.get_future();
    EXPECT(forwardingStarted.wait_for(std::chrono::seconds(10)) == std::future_status::ready);

    // the connection to the store is lost: the flush fails now, it does not wait for the catalogue
    locations.failPending(serverFailure());
    EXPECT(waiter.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
    EXPECT_THROWS_AS(static_cast<void>(waiter.get()), eckit::SeriousBug);

    answer.now();
}

CASE("Locations: a connection lost before registration rejects a later archive") {
    remote::Locations locations;

    locations.failPending(serverFailure());
    EXPECT_THROWS_AS(archive(locations, 1), eckit::SeriousBug);
    EXPECT(locations.archived() == 0);
}

CASE("FDB destruction logs a repeated flush failure instead of terminating") {
    static FDBBuilder<FailingFlushFDB> builder("test-failing-flush");
    FailingFlushFDB::flushes = 0;
    Config config;
    config.set("type", "test-failing-flush");

    {
        FDB fdb(config);
        const char data = 'x';
        fdb.archive(Key{}, &data, 1);
        EXPECT_THROWS_AS(fdb.flush(), eckit::SeriousBug);
    }

    EXPECT(FailingFlushFDB::flushes == 2);
}

CASE("Archiver destruction attempts every database after a flush failure") {
    int flushes = 0;
    {
        TestArchiver archiver(flushes);
        EXPECT_THROWS_AS(archiver.flush(), eckit::SeriousBug);
    }
    EXPECT(flushes == 3);
}

}  // namespace fdb5::test

//-----------------------------------------------------------------------------

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
