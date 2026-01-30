/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#include <cstdlib>

#include "eckit/testing/Test.h"

#include "fdb5/api/helpers/WipeIterator.h"
#include "metkit/mars/TypeAny.h"

#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/config/Config.h"

#include "ApiSpy.h"

using namespace eckit::testing;
using namespace eckit;


namespace fdb {
namespace test {

//----------------------------------------------------------------------------------------------------------------------


fdb5::Config defaultConfig() {

    // Build a standard configuration to demonstrate features of selectFDB

    LocalConfiguration cfg_od;
    cfg_od.set("type", "spy");
    cfg_od.set("select", "class=od");

    LocalConfiguration cfg_rd1;
    cfg_rd1.set("type", "spy");
    cfg_rd1.set("select", "class=rd,expver=xx.?.?");

    LocalConfiguration cfg_rd2;
    cfg_rd2.set("type", "spy");
    cfg_rd2.set("select", "class=rd,expver=yy.?.?");

    fdb5::Config cfg;
    cfg.set("type", "select");
    cfg.set("fdbs", {cfg_od, cfg_rd1, cfg_rd2});

    return cfg;
}


CASE("archives_distributed_according_to_select") {

    ApiSpy::knownSpies().clear();

    // Build FDB from default config

    fdb5::FDB fdb(defaultConfig());
    EXPECT(ApiSpy::knownSpies().size() == 0);  // lazy creation

    // Flush does nothing until dirty
    fdb.flush();

    EXPECT(ApiSpy::knownSpies().size() == 0);

    // Do some archiving

    fdb5::Key k;
    k.set("class", "od");
    k.set("expver", "xxxx");

    fdb.archive(k, (const void*)0x1234, 1234);

    EXPECT(ApiSpy::knownSpies().size() == 1);

    ApiSpy& spy_od(*ApiSpy::knownSpies()[0]);

    EXPECT(spy_od.counts().archive == 1);
    EXPECT(spy_od.counts().flush == 0);

    k.set("class", "rd");
    k.set("expver", "yyyy");

    fdb.archive(k, (const void*)0x4321, 4321);

    EXPECT(ApiSpy::knownSpies().size() == 2);

    ApiSpy& spy_rd2(*ApiSpy::knownSpies()[1]);

    EXPECT(spy_od.counts().archive == 1);
    EXPECT(spy_od.counts().flush == 0);
    EXPECT(spy_rd2.counts().archive == 1);
    EXPECT(spy_rd2.counts().flush == 0);

    fdb.flush();

    EXPECT(spy_od.counts().archive == 1);
    EXPECT(spy_od.counts().flush == 1);
    EXPECT(spy_rd2.counts().archive == 1);
    EXPECT(spy_rd2.counts().flush == 1);

    // Check that the API calls were forwarded correctly

    fdb5::Key key;
    const void* ptr;
    size_t len;

    std::tie(key, ptr, len) = spy_od.archives()[0];
    EXPECT(key.size() == 2);
    EXPECT(key.value("class") == "od");
    EXPECT(key.value("expver") == "xxxx");
    EXPECT(ptr == (void*)0x1234);
    EXPECT(len == 1234);

    std::tie(key, ptr, len) = spy_rd2.archives()[0];
    EXPECT(key.size() == 2);
    EXPECT(key.value("class") == "rd");
    EXPECT(key.value("expver") == "yyyy");
    EXPECT(ptr == (void*)0x4321);
    EXPECT(len == 4321);

    // And unused functions

    ApiSpy* spies[] = {&spy_od, &spy_rd2};
    for (int i = 0; i < 2; i++) {
        ApiSpy* spy = spies[i];
        EXPECT(spy->counts().inspect == 0);
        EXPECT(spy->counts().list == 0);
        EXPECT(spy->counts().dump == 0);
        EXPECT(spy->counts().status == 0);
        EXPECT(spy->counts().wipe == 0);
        EXPECT(spy->counts().purge == 0);
        EXPECT(spy->counts().stats == 0);
        EXPECT(spy->counts().control == 0);
    }
}


CASE("retrieves_distributed_according_to_select") {

    ApiSpy::knownSpies().clear();

    // Build FDB from default config

    fdb5::FDB fdb(defaultConfig());
    fdb.list(metkit::mars::MarsRequest{"class=od"});
    fdb.list(metkit::mars::MarsRequest{"class=rd"});

    EXPECT(ApiSpy::knownSpies().size() == 3);
    ApiSpy& spy_od(*ApiSpy::knownSpies()[0]);
    ApiSpy& spy_rd1(*ApiSpy::knownSpies()[1]);
    ApiSpy& spy_rd2(*ApiSpy::knownSpies()[2]);

    // Do some archiving

    metkit::mars::MarsRequest req;
    req.setValuesTyped(new metkit::mars::TypeAny("class"), std::vector<std::string>{"od"});
    req.setValuesTyped(new metkit::mars::TypeAny("expver"), std::vector<std::string>{"xxxx"});
    fdb.inspect(req);

    EXPECT(spy_od.counts().inspect == 1);
    EXPECT(spy_rd1.counts().inspect == 0);
    EXPECT(spy_rd2.counts().inspect == 0);

    req.setValuesTyped(new metkit::mars::TypeAny("class"), std::vector<std::string>{std::string("rd")});
    fdb.inspect(req);

    EXPECT(spy_od.counts().inspect == 1);
    EXPECT(spy_rd1.counts().inspect == 1);
    EXPECT(spy_rd2.counts().inspect == 0);

    // Under specified - matches nothing. Requests halted at this point, as FDB retrieves need
    // to be fully specified

    req.unsetValues("expver");
    fdb.inspect(req);

    EXPECT(spy_od.counts().inspect == 1);
    EXPECT(spy_rd1.counts().inspect == 1);
    EXPECT(spy_rd2.counts().inspect == 0);

    // Now match all the rd lanes

    req.setValuesTyped(new metkit::mars::TypeAny("expver"), std::vector<std::string>{"xx12", "yy21"});
    fdb.inspect(req);

    EXPECT(spy_od.counts().inspect == 1);
    EXPECT(spy_rd1.counts().inspect == 2);
    EXPECT(spy_rd2.counts().inspect == 1);

    req.setValuesTyped(new metkit::mars::TypeAny("class"), std::vector<std::string>{"od", "rd"});
    fdb.inspect(req);

    EXPECT(spy_od.counts().inspect == 2);
    EXPECT(spy_rd1.counts().inspect == 3);
    EXPECT(spy_rd2.counts().inspect == 2);

    // And unused functions

    ApiSpy* spies[] = {&spy_od, &spy_rd1, &spy_rd2};
    for (int i = 0; i < 3; i++) {
        ApiSpy* spy = spies[i];
        EXPECT(spy->counts().archive == 0);
        EXPECT(spy->counts().flush == 0);
        EXPECT(spy->counts().dump == 0);
        EXPECT(spy->counts().status == 0);
        EXPECT(spy->counts().wipe == 0);
        EXPECT(spy->counts().purge == 0);
        EXPECT(spy->counts().stats == 0);
        EXPECT(spy->counts().control == 0);
    }
}

CASE("lists_distributed_according_to_select") {

    ApiSpy::knownSpies().clear();

    // Build FDB from default config

    fdb5::FDB fdb(defaultConfig());
    fdb.list(fdb5::FDBToolRequest{{}, true});

    EXPECT(ApiSpy::knownSpies().size() == 3);
    ApiSpy& spy_od(*ApiSpy::knownSpies()[0]);
    ApiSpy& spy_rd1(*ApiSpy::knownSpies()[1]);
    ApiSpy& spy_rd2(*ApiSpy::knownSpies()[2]);

    // Do some archiving

    fdb.list(fdb5::FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0]);

    EXPECT(spy_od.counts().list == 2);
    EXPECT(spy_rd1.counts().list == 1);
    EXPECT(spy_rd2.counts().list == 1);

    fdb.list(fdb5::FDBToolRequest::requestsFromString("class=rd,expver=xxxx")[0]);

    EXPECT(spy_od.counts().list == 2);
    EXPECT(spy_rd1.counts().list == 2);
    EXPECT(spy_rd2.counts().list == 1);

    // Under specified - matches nothing. Requests halted at this point, as FDB retrieves need
    // to be fully specified

    fdb.list(fdb5::FDBToolRequest::requestsFromString("class=rd,expver=zzzz")[0]);

    EXPECT(spy_od.counts().list == 2);
    EXPECT(spy_rd1.counts().list == 2);
    EXPECT(spy_rd2.counts().list == 1);

    //// Now match all the rd lanes

    fdb.list(fdb5::FDBToolRequest::requestsFromString("class=rd")[0]);

    EXPECT(spy_od.counts().list == 2);
    EXPECT(spy_rd1.counts().list == 3);
    EXPECT(spy_rd2.counts().list == 2);

    // Explicitly match everything

    fdb.list(fdb5::FDBToolRequest({}, true));

    EXPECT(spy_od.counts().list == 3);
    EXPECT(spy_rd1.counts().list == 4);
    EXPECT(spy_rd2.counts().list == 3);

    // And unused functions

    ApiSpy* spies[] = {&spy_od, &spy_rd1, &spy_rd2};
    for (int i = 0; i < 3; i++) {
        ApiSpy* spy = spies[i];
        EXPECT(spy->counts().archive == 0);
        EXPECT(spy->counts().flush == 0);
        EXPECT(spy->counts().inspect == 0);
        EXPECT(spy->counts().dump == 0);
        EXPECT(spy->counts().status == 0);
        EXPECT(spy->counts().wipe == 0);
        EXPECT(spy->counts().purge == 0);
        EXPECT(spy->counts().stats == 0);
        EXPECT(spy->counts().control == 0);
    }
}


CASE("dump_distributed_according_to_select") {

    ApiSpy::knownSpies().clear();

    // Build FDB from default config

    fdb5::FDB fdb(defaultConfig());
    fdb.list(fdb5::FDBToolRequest{{}, true, {}});

    EXPECT(ApiSpy::knownSpies().size() == 3);
    ApiSpy& spy_od(*ApiSpy::knownSpies()[0]);
    ApiSpy& spy_rd1(*ApiSpy::knownSpies()[1]);
    ApiSpy& spy_rd2(*ApiSpy::knownSpies()[2]);

    // Do some archiving

    fdb.dump(fdb5::FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0]);

    EXPECT(spy_od.counts().dump == 1);
    EXPECT(spy_rd1.counts().dump == 0);
    EXPECT(spy_rd2.counts().dump == 0);

    fdb.dump(fdb5::FDBToolRequest::requestsFromString("class=rd,expver=xxxx")[0]);

    EXPECT(spy_od.counts().dump == 1);
    EXPECT(spy_rd1.counts().dump == 1);
    EXPECT(spy_rd2.counts().dump == 0);

    // Under specified - matches nothing. Requests halted at this point, as FDB retrieves need
    // to be fully specified

    fdb.dump(fdb5::FDBToolRequest::requestsFromString("class=rd,expver=zzzz")[0]);

    EXPECT(spy_od.counts().dump == 1);
    EXPECT(spy_rd1.counts().dump == 1);
    EXPECT(spy_rd2.counts().dump == 0);

    //// Now match all the rd lanes
    fdb.dump(fdb5::FDBToolRequest::requestsFromString("class=rd")[0]);

    EXPECT(spy_od.counts().dump == 1);
    EXPECT(spy_rd1.counts().dump == 2);
    EXPECT(spy_rd2.counts().dump == 1);

    // Explicitly match everything

    fdb.dump(fdb5::FDBToolRequest({}, true));

    EXPECT(spy_od.counts().dump == 2);
    EXPECT(spy_rd1.counts().dump == 3);
    EXPECT(spy_rd2.counts().dump == 2);

    // And unused functions

    ApiSpy* spies[] = {&spy_od, &spy_rd1, &spy_rd2};
    for (int i = 0; i < 3; i++) {
        ApiSpy* spy = spies[i];
        EXPECT(spy->counts().archive == 0);
        EXPECT(spy->counts().flush == 0);
        EXPECT(spy->counts().inspect == 0);
        EXPECT(spy->counts().list == 1);
        EXPECT(spy->counts().status == 0);
        EXPECT(spy->counts().wipe == 0);
        EXPECT(spy->counts().purge == 0);
        EXPECT(spy->counts().stats == 0);
        EXPECT(spy->counts().control == 0);
    }
}

CASE("status_distributed_according_to_select") {

    ApiSpy::knownSpies().clear();

    // Build FDB from default config

    fdb5::FDB fdb(defaultConfig());
    fdb.list(fdb5::FDBToolRequest{{}, true, {}});

    EXPECT(ApiSpy::knownSpies().size() == 3);
    ApiSpy& spy_od(*ApiSpy::knownSpies()[0]);
    ApiSpy& spy_rd1(*ApiSpy::knownSpies()[1]);
    ApiSpy& spy_rd2(*ApiSpy::knownSpies()[2]);

    // Do some archiving

    fdb.status(fdb5::FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0]);

    EXPECT(spy_od.counts().status == 1);
    EXPECT(spy_rd1.counts().status == 0);
    EXPECT(spy_rd2.counts().status == 0);

    fdb.status(fdb5::FDBToolRequest::requestsFromString("class=rd,expver=xxxx")[0]);

    EXPECT(spy_od.counts().status == 1);
    EXPECT(spy_rd1.counts().status == 1);
    EXPECT(spy_rd2.counts().status == 0);

    // Under specified - matches nothing. Requests halted at this point, as FDB retrieves need
    // to be fully specified

    fdb.status(fdb5::FDBToolRequest::requestsFromString("class=rd,expver=zzzz")[0]);

    EXPECT(spy_od.counts().status == 1);
    EXPECT(spy_rd1.counts().status == 1);
    EXPECT(spy_rd2.counts().status == 0);

    //// Now match all the rd lanes

    fdb.status(fdb5::FDBToolRequest::requestsFromString("class=rd")[0]);

    EXPECT(spy_od.counts().status == 1);
    EXPECT(spy_rd1.counts().status == 2);
    EXPECT(spy_rd2.counts().status == 1);

    // Explicitly match everything

    fdb.status(fdb5::FDBToolRequest({}, true));

    EXPECT(spy_od.counts().status == 2);
    EXPECT(spy_rd1.counts().status == 3);
    EXPECT(spy_rd2.counts().status == 2);

    // And unused functions

    ApiSpy* spies[] = {&spy_od, &spy_rd1, &spy_rd2};
    for (int i = 0; i < 3; i++) {
        ApiSpy* spy = spies[i];
        EXPECT(spy->counts().archive == 0);
        EXPECT(spy->counts().flush == 0);
        EXPECT(spy->counts().inspect == 0);
        EXPECT(spy->counts().list == 1);
        EXPECT(spy->counts().dump == 0);
        EXPECT(spy->counts().wipe == 0);
        EXPECT(spy->counts().purge == 0);
        EXPECT(spy->counts().stats == 0);
        EXPECT(spy->counts().control == 0);
    }
}


CASE("wipe_distributed_according_to_select") {

    ApiSpy::knownSpies().clear();

    // Build FDB from default config

    fdb5::FDB fdb(defaultConfig());
    fdb.list(fdb5::FDBToolRequest{{}, true, {}});

    EXPECT(ApiSpy::knownSpies().size() == 3);
    ApiSpy& spy_od(*ApiSpy::knownSpies()[0]);
    ApiSpy& spy_rd1(*ApiSpy::knownSpies()[1]);
    ApiSpy& spy_rd2(*ApiSpy::knownSpies()[2]);

    fdb.wipe(fdb5::FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0]);

    EXPECT_EQUAL(spy_od.counts().wipe, 1);
    EXPECT_EQUAL(spy_rd1.counts().wipe, 0);
    EXPECT_EQUAL(spy_rd2.counts().wipe, 0);

    fdb.wipe(fdb5::FDBToolRequest::requestsFromString("class=rd,expver=xxxx")[0]);

    EXPECT_EQUAL(spy_od.counts().wipe, 1);
    EXPECT_EQUAL(spy_rd1.counts().wipe, 1);
    EXPECT_EQUAL(spy_rd2.counts().wipe, 0);

    // Under specified - matches nothing. Requests halted at this point, as FDB retrieves need
    // to be fully specified

    fdb.wipe(fdb5::FDBToolRequest::requestsFromString("class=rd,expver=zzzz")[0]);

    EXPECT_EQUAL(spy_od.counts().wipe, 1);
    EXPECT_EQUAL(spy_rd1.counts().wipe, 1);
    EXPECT_EQUAL(spy_rd2.counts().wipe, 0);

    // Now match all the rd lanes
    // We currently prohibit wipes that match multiple lanes, check that we raise an error

    bool caught = false;
    try {

        auto x = fdb.wipe(fdb5::FDBToolRequest::requestsFromString("class=rd")[0]);
        fdb5::WipeElement e;
        while (x.next(e)) {
        }

    } catch (eckit::UserError& err) {
        caught = true;
    }

    EXPECT(caught);

    // Explicitly match everything
    // We currently prohibit wipes that match multiple lanes, check that we raise an error
    caught = false;
    try {
        auto x = fdb.wipe(fdb5::FDBToolRequest({}, true));
        fdb5::WipeElement e;
        while (x.next(e)) {
        }
    } catch (eckit::UserError& err) {
        caught = true;
    }
    EXPECT(caught);

    // And unused functions

    ApiSpy* spies[] = {&spy_od, &spy_rd1, &spy_rd2};
    for (int i = 0; i < 3; i++) {
        ApiSpy* spy = spies[i];
        EXPECT(spy->counts().archive == 0);
        EXPECT(spy->counts().flush == 0);
        EXPECT(spy->counts().inspect == 0);
        EXPECT(spy->counts().list == 1);
        EXPECT(spy->counts().dump == 0);
        EXPECT(spy->counts().status == 0);
        EXPECT(spy->counts().purge == 0);
        EXPECT(spy->counts().stats == 0);
        EXPECT(spy->counts().control == 0);
    }
}


CASE("purge_distributed_according_to_select") {

    ApiSpy::knownSpies().clear();

    // Build FDB from default config

    fdb5::FDB fdb(defaultConfig());
    fdb.list(fdb5::FDBToolRequest{{}, true, {}});

    EXPECT(ApiSpy::knownSpies().size() == 3);
    ApiSpy& spy_od(*ApiSpy::knownSpies()[0]);
    ApiSpy& spy_rd1(*ApiSpy::knownSpies()[1]);
    ApiSpy& spy_rd2(*ApiSpy::knownSpies()[2]);

    // Do some archiving

    fdb.purge(fdb5::FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0]);

    EXPECT(spy_od.counts().purge == 1);
    EXPECT(spy_rd1.counts().purge == 0);
    EXPECT(spy_rd2.counts().purge == 0);

    fdb.purge(fdb5::FDBToolRequest::requestsFromString("class=rd,expver=xxxx")[0]);

    EXPECT(spy_od.counts().purge == 1);
    EXPECT(spy_rd1.counts().purge == 1);
    EXPECT(spy_rd2.counts().purge == 0);

    // Under specified - matches nothing. Requests halted at this point, as FDB retrieves need
    // to be fully specified

    fdb.purge(fdb5::FDBToolRequest::requestsFromString("class=rd,expver=zzzz")[0]);

    EXPECT(spy_od.counts().purge == 1);
    EXPECT(spy_rd1.counts().purge == 1);
    EXPECT(spy_rd2.counts().purge == 0);

    //// Now match all the rd lanes

    fdb.purge(fdb5::FDBToolRequest::requestsFromString("class=rd")[0]);

    EXPECT(spy_od.counts().purge == 1);
    EXPECT(spy_rd1.counts().purge == 2);
    EXPECT(spy_rd2.counts().purge == 1);

    // Explicitly match everything

    fdb.purge(fdb5::FDBToolRequest({}, true));

    EXPECT(spy_od.counts().purge == 2);
    EXPECT(spy_rd1.counts().purge == 3);
    EXPECT(spy_rd2.counts().purge == 2);

    // And unused functions

    ApiSpy* spies[] = {&spy_od, &spy_rd1, &spy_rd2};
    for (int i = 0; i < 3; i++) {
        ApiSpy* spy = spies[i];
        EXPECT(spy->counts().archive == 0);
        EXPECT(spy->counts().flush == 0);
        EXPECT(spy->counts().inspect == 0);
        EXPECT(spy->counts().list == 1);
        EXPECT(spy->counts().dump == 0);
        EXPECT(spy->counts().status == 0);
        EXPECT(spy->counts().wipe == 0);
        EXPECT(spy->counts().stats == 0);
        EXPECT(spy->counts().control == 0);
    }
}


CASE("stats_distributed_according_to_select") {

    ApiSpy::knownSpies().clear();

    // Build FDB from default config

    fdb5::FDB fdb(defaultConfig());
    fdb.list(fdb5::FDBToolRequest{{}, true, {}});

    EXPECT(ApiSpy::knownSpies().size() == 3);
    ApiSpy& spy_od(*ApiSpy::knownSpies()[0]);
    ApiSpy& spy_rd1(*ApiSpy::knownSpies()[1]);
    ApiSpy& spy_rd2(*ApiSpy::knownSpies()[2]);

    // Do some archiving

    fdb.stats(fdb5::FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0]);

    EXPECT(spy_od.counts().stats == 1);
    EXPECT(spy_rd1.counts().stats == 0);
    EXPECT(spy_rd2.counts().stats == 0);

    fdb.stats(fdb5::FDBToolRequest::requestsFromString("class=rd,expver=xxxx")[0]);

    EXPECT(spy_od.counts().stats == 1);
    EXPECT(spy_rd1.counts().stats == 1);
    EXPECT(spy_rd2.counts().stats == 0);

    // Under specified - matches nothing. Requests halted at this point, as FDB retrieves need
    // to be fully specified

    fdb.stats(fdb5::FDBToolRequest::requestsFromString("class=rd,expver=zzzz")[0]);

    EXPECT(spy_od.counts().stats == 1);
    EXPECT(spy_rd1.counts().stats == 1);
    EXPECT(spy_rd2.counts().stats == 0);

    //// Now match all the rd lanes

    fdb.stats(fdb5::FDBToolRequest::requestsFromString("class=rd")[0]);

    EXPECT(spy_od.counts().stats == 1);
    EXPECT(spy_rd1.counts().stats == 2);
    EXPECT(spy_rd2.counts().stats == 1);

    // Explicitly match everything

    fdb.stats(fdb5::FDBToolRequest({}, true));

    EXPECT(spy_od.counts().stats == 2);
    EXPECT(spy_rd1.counts().stats == 3);
    EXPECT(spy_rd2.counts().stats == 2);

    // And unused functions

    ApiSpy* spies[] = {&spy_od, &spy_rd1, &spy_rd2};
    for (int i = 0; i < 3; i++) {
        ApiSpy* spy = spies[i];
        EXPECT(spy->counts().archive == 0);
        EXPECT(spy->counts().flush == 0);
        EXPECT(spy->counts().inspect == 0);
        EXPECT(spy->counts().list == 1);
        EXPECT(spy->counts().dump == 0);
        EXPECT(spy->counts().status == 0);
        EXPECT(spy->counts().wipe == 0);
        EXPECT(spy->counts().purge == 0);
        EXPECT(spy->counts().control == 0);
    }
}


CASE("control_distributed_according_to_select") {

    ApiSpy::knownSpies().clear();

    // Build FDB from default config

    fdb5::FDB fdb(defaultConfig());
    fdb.list(fdb5::FDBToolRequest{{}, true, {}});

    EXPECT(ApiSpy::knownSpies().size() == 3);
    ApiSpy& spy_od(*ApiSpy::knownSpies()[0]);
    ApiSpy& spy_rd1(*ApiSpy::knownSpies()[1]);
    ApiSpy& spy_rd2(*ApiSpy::knownSpies()[2]);

    // Do some archiving

    fdb.control(fdb5::FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0], fdb5::ControlAction::Disable,
                fdb5::ControlIdentifiers(fdb5::ControlIdentifier::List));

    EXPECT(spy_od.counts().control == 1);
    EXPECT(spy_rd1.counts().control == 0);
    EXPECT(spy_rd2.counts().control == 0);

    fdb.control(fdb5::FDBToolRequest::requestsFromString("class=rd,expver=xxxx")[0], fdb5::ControlAction::Disable,
                fdb5::ControlIdentifiers(fdb5::ControlIdentifier::Wipe));

    EXPECT(spy_od.counts().control == 1);
    EXPECT(spy_rd1.counts().control == 1);
    EXPECT(spy_rd2.counts().control == 0);

    // Under specified - matches nothing. Requests halted at this point, as FDB retrieves need
    // to be fully specified

    fdb.control(fdb5::FDBToolRequest::requestsFromString("class=rd,expver=zzzz")[0], fdb5::ControlAction::Enable,
                fdb5::ControlIdentifiers(fdb5::ControlIdentifier::Retrieve));

    EXPECT(spy_od.counts().control == 1);
    EXPECT(spy_rd1.counts().control == 1);
    EXPECT(spy_rd2.counts().control == 0);

    //// Now match all the rd lanes

    fdb.control(fdb5::FDBToolRequest::requestsFromString("class=rd")[0], fdb5::ControlAction::Enable,
                fdb5::ControlIdentifiers(fdb5::ControlIdentifier::Archive));

    EXPECT(spy_od.counts().control == 1);
    EXPECT(spy_rd1.counts().control == 2);
    EXPECT(spy_rd2.counts().control == 1);

    // Explicitly match everything

    fdb.control(fdb5::FDBToolRequest({}, true), fdb5::ControlAction::Disable,
                fdb5::ControlIdentifiers(fdb5::ControlIdentifier::List));

    EXPECT(spy_od.counts().control == 2);
    EXPECT(spy_rd1.counts().control == 3);
    EXPECT(spy_rd2.counts().control == 2);

    // And unused functions

    for (auto spy : {&spy_od, &spy_rd1, &spy_rd2}) {
        EXPECT(spy->counts().archive == 0);
        EXPECT(spy->counts().flush == 0);
        EXPECT(spy->counts().inspect == 0);
        EXPECT(spy->counts().list == 1);
        EXPECT(spy->counts().dump == 0);
        EXPECT(spy->counts().status == 0);
        EXPECT(spy->counts().wipe == 0);
        EXPECT(spy->counts().purge == 0);
        EXPECT(spy->counts().stats == 0);
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace test
}  // namespace fdb

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
