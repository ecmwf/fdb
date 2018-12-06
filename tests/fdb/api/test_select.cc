/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <cstdlib>

#include "eckit/config/Resource.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/io/DataHandle.h"
#include "eckit/testing/Test.h"

#include "fdb5/config/Config.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

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
    cfg.set("fdbs", { cfg_od, cfg_rd1, cfg_rd2 });

    return cfg;
}


CASE( "archives_distributed_according_to_select" ) {

    // Build FDB from default config

    fdb5::FDB fdb(defaultConfig());
    EXPECT(ApiSpy::knownSpies().size() == 3);
    ApiSpy& spy_od(*ApiSpy::knownSpies()[0]);
    ApiSpy& spy_rd1(*ApiSpy::knownSpies()[1]);
    ApiSpy& spy_rd2(*ApiSpy::knownSpies()[2]);

    // Flush does nothing until dirty

    fdb.flush();

    EXPECT(spy_od.counts().flush == 0);
    EXPECT(spy_rd1.counts().flush == 0);
    EXPECT(spy_rd2.counts().flush == 0);

    // Do some archiving

    fdb5::Key k;
    k.set("class", "od");
    k.set("expver", "xxxx");
    fdb.archive(k, (const void*)0x1234, 1234);

    EXPECT(spy_od.counts().archive == 1);
    EXPECT(spy_od.counts().flush == 0);
    EXPECT(spy_rd1.counts().archive == 0);
    EXPECT(spy_rd1.counts().flush == 0);
    EXPECT(spy_rd2.counts().archive == 0);
    EXPECT(spy_rd2.counts().flush == 0);

    k.set("class", "rd");
    k.set("expver", "yyyy");
    fdb.archive(k, (const void*)0x4321, 4321);

    EXPECT(spy_od.counts().archive == 1);
    EXPECT(spy_od.counts().flush == 0);
    EXPECT(spy_rd1.counts().archive == 0);
    EXPECT(spy_rd1.counts().flush == 0);
    EXPECT(spy_rd2.counts().archive == 1);
    EXPECT(spy_rd2.counts().flush == 0);

    fdb.flush();

    EXPECT(spy_od.counts().archive == 1);
    EXPECT(spy_od.counts().flush == 1);
    EXPECT(spy_rd1.counts().archive == 0);
    EXPECT(spy_rd1.counts().flush == 0);
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

    for (auto spy : {&spy_od, &spy_rd1, &spy_rd2}) {
        EXPECT(spy->counts().retrieve == 0);
        EXPECT(spy->counts().list == 0);
        EXPECT(spy->counts().dump == 0);
        EXPECT(spy->counts().where == 0);
        EXPECT(spy->counts().wipe == 0);
        EXPECT(spy->counts().purge == 0);
        EXPECT(spy->counts().stats == 0);
    }
}


CASE( "retrieves_distributed_according_to_select" ) {

    // Build FDB from default config

    fdb5::FDB fdb(defaultConfig());
    EXPECT(ApiSpy::knownSpies().size() == 3);
    ApiSpy& spy_od(*ApiSpy::knownSpies()[0]);
    ApiSpy& spy_rd1(*ApiSpy::knownSpies()[1]);
    ApiSpy& spy_rd2(*ApiSpy::knownSpies()[2]);

    // Do some archiving

    MarsRequest req;
    req.setValues("class", std::vector<std::string>{"od"});
    req.setValues("expver", std::vector<std::string>{"xxxx"});
    fdb.retrieve(req);

    EXPECT(spy_od.counts().retrieve == 1);
    EXPECT(spy_rd1.counts().retrieve == 0);
    EXPECT(spy_rd2.counts().retrieve == 0);

    req.setValues("class", std::vector<std::string>{std::string("rd")});
    fdb.retrieve(req);

    EXPECT(spy_od.counts().retrieve == 1);
    EXPECT(spy_rd1.counts().retrieve == 1);
    EXPECT(spy_rd2.counts().retrieve == 0);

    // Under specified - matches nothing. Requests halted at this point, as FDB retrieves need
    // to be fully specified

    req.unsetValues("expver");
    fdb.retrieve(req);

    EXPECT(spy_od.counts().retrieve == 1);
    EXPECT(spy_rd1.counts().retrieve == 1);
    EXPECT(spy_rd2.counts().retrieve == 0);

    // Now match all the rd lanes

    req.setValues("expver", std::vector<std::string>{"xx12", "yy21"});
    fdb.retrieve(req);

    EXPECT(spy_od.counts().retrieve == 1);
    EXPECT(spy_rd1.counts().retrieve == 2);
    EXPECT(spy_rd2.counts().retrieve == 1);

    req.setValues("class", std::vector<std::string>{"od", "rd"});
    fdb.retrieve(req);

    EXPECT(spy_od.counts().retrieve == 2);
    EXPECT(spy_rd1.counts().retrieve == 3);
    EXPECT(spy_rd2.counts().retrieve == 2);

    // And unused functions

    for (auto spy : {&spy_od, &spy_rd1, &spy_rd2}) {
        EXPECT(spy->counts().archive == 0);
        EXPECT(spy->counts().flush == 0);
        EXPECT(spy->counts().list == 0);
        EXPECT(spy->counts().dump == 0);
        EXPECT(spy->counts().where == 0);
        EXPECT(spy->counts().wipe == 0);
        EXPECT(spy->counts().purge == 0);
        EXPECT(spy->counts().stats == 0);
    }
}

CASE( "lists_distributed_according_to_select" ) {

    // Build FDB from default config

    fdb5::FDB fdb(defaultConfig());
    EXPECT(ApiSpy::knownSpies().size() == 3);
    ApiSpy& spy_od(*ApiSpy::knownSpies()[0]);
    ApiSpy& spy_rd1(*ApiSpy::knownSpies()[1]);
    ApiSpy& spy_rd2(*ApiSpy::knownSpies()[2]);

    // Do some archiving

    fdb.list(fdb5::FDBToolRequest("class=od,expver=xxxx", false));

    EXPECT(spy_od.counts().list == 1);
    EXPECT(spy_rd1.counts().list == 0);
    EXPECT(spy_rd2.counts().list == 0);

    fdb.list(fdb5::FDBToolRequest("class=rd,expver=xxxx", false));

    EXPECT(spy_od.counts().list == 1);
    EXPECT(spy_rd1.counts().list == 1);
    EXPECT(spy_rd2.counts().list == 0);

    // Under specified - matches nothing. Requests halted at this point, as FDB retrieves need
    // to be fully specified

    fdb.list(fdb5::FDBToolRequest("class=rd,expver=zzzz", false));

    EXPECT(spy_od.counts().list == 1);
    EXPECT(spy_rd1.counts().list == 1);
    EXPECT(spy_rd2.counts().list == 0);

    //// Now match all the rd lanes

    fdb.list(fdb5::FDBToolRequest("class=rd", false));

    EXPECT(spy_od.counts().list == 1);
    EXPECT(spy_rd1.counts().list == 2);
    EXPECT(spy_rd2.counts().list == 1);

    // Explicitly match everything

    fdb.list(fdb5::FDBToolRequest("", true));

    EXPECT(spy_od.counts().list == 2);
    EXPECT(spy_rd1.counts().list == 3);
    EXPECT(spy_rd2.counts().list == 2);

    // And unused functions

    for (auto spy : {&spy_od, &spy_rd1, &spy_rd2}) {
        EXPECT(spy->counts().archive == 0);
        EXPECT(spy->counts().flush == 0);
        EXPECT(spy->counts().retrieve == 0);
        EXPECT(spy->counts().dump == 0);
        EXPECT(spy->counts().where == 0);
        EXPECT(spy->counts().wipe == 0);
        EXPECT(spy->counts().purge == 0);
        EXPECT(spy->counts().stats == 0);
    }
}


CASE( "dump_distributed_according_to_select" ) {

    // Build FDB from default config

    fdb5::FDB fdb(defaultConfig());
    EXPECT(ApiSpy::knownSpies().size() == 3);
    ApiSpy& spy_od(*ApiSpy::knownSpies()[0]);
    ApiSpy& spy_rd1(*ApiSpy::knownSpies()[1]);
    ApiSpy& spy_rd2(*ApiSpy::knownSpies()[2]);

    // Do some archiving

    fdb.dump(fdb5::FDBToolRequest("class=od,expver=xxxx", false));

    EXPECT(spy_od.counts().dump == 1);
    EXPECT(spy_rd1.counts().dump == 0);
    EXPECT(spy_rd2.counts().dump == 0);

    fdb.dump(fdb5::FDBToolRequest("class=rd,expver=xxxx", false));

    EXPECT(spy_od.counts().dump == 1);
    EXPECT(spy_rd1.counts().dump == 1);
    EXPECT(spy_rd2.counts().dump == 0);

    // Under specified - matches nothing. Requests halted at this point, as FDB retrieves need
    // to be fully specified

    fdb.dump(fdb5::FDBToolRequest("class=rd,expver=zzzz", false));

    EXPECT(spy_od.counts().dump == 1);
    EXPECT(spy_rd1.counts().dump == 1);
    EXPECT(spy_rd2.counts().dump == 0);

    //// Now match all the rd lanes

    fdb.dump(fdb5::FDBToolRequest("class=rd", false));

    EXPECT(spy_od.counts().dump == 1);
    EXPECT(spy_rd1.counts().dump == 2);
    EXPECT(spy_rd2.counts().dump == 1);

    // Explicitly match everything

    fdb.dump(fdb5::FDBToolRequest("", true));

    EXPECT(spy_od.counts().dump == 2);
    EXPECT(spy_rd1.counts().dump == 3);
    EXPECT(spy_rd2.counts().dump == 2);

    // And unused functions

    for (auto spy : {&spy_od, &spy_rd1, &spy_rd2}) {
        EXPECT(spy->counts().archive == 0);
        EXPECT(spy->counts().flush == 0);
        EXPECT(spy->counts().retrieve == 0);
        EXPECT(spy->counts().list == 0);
        EXPECT(spy->counts().where == 0);
        EXPECT(spy->counts().wipe == 0);
        EXPECT(spy->counts().purge == 0);
        EXPECT(spy->counts().stats == 0);
    }
}

CASE( "where_distributed_according_to_select" ) {

    // Build FDB from default config

    fdb5::FDB fdb(defaultConfig());
    EXPECT(ApiSpy::knownSpies().size() == 3);
    ApiSpy& spy_od(*ApiSpy::knownSpies()[0]);
    ApiSpy& spy_rd1(*ApiSpy::knownSpies()[1]);
    ApiSpy& spy_rd2(*ApiSpy::knownSpies()[2]);

    // Do some archiving

    fdb.where(fdb5::FDBToolRequest("class=od,expver=xxxx", false));

    EXPECT(spy_od.counts().where == 1);
    EXPECT(spy_rd1.counts().where == 0);
    EXPECT(spy_rd2.counts().where == 0);

    fdb.where(fdb5::FDBToolRequest("class=rd,expver=xxxx", false));

    EXPECT(spy_od.counts().where == 1);
    EXPECT(spy_rd1.counts().where == 1);
    EXPECT(spy_rd2.counts().where == 0);

    // Under specified - matches nothing. Requests halted at this point, as FDB retrieves need
    // to be fully specified

    fdb.where(fdb5::FDBToolRequest("class=rd,expver=zzzz", false));

    EXPECT(spy_od.counts().where == 1);
    EXPECT(spy_rd1.counts().where == 1);
    EXPECT(spy_rd2.counts().where == 0);

    //// Now match all the rd lanes

    fdb.where(fdb5::FDBToolRequest("class=rd", false));

    EXPECT(spy_od.counts().where == 1);
    EXPECT(spy_rd1.counts().where == 2);
    EXPECT(spy_rd2.counts().where == 1);

    // Explicitly match everything

    fdb.where(fdb5::FDBToolRequest("", true));

    EXPECT(spy_od.counts().where == 2);
    EXPECT(spy_rd1.counts().where == 3);
    EXPECT(spy_rd2.counts().where == 2);

    // And unused functions

    for (auto spy : {&spy_od, &spy_rd1, &spy_rd2}) {
        EXPECT(spy->counts().archive == 0);
        EXPECT(spy->counts().flush == 0);
        EXPECT(spy->counts().retrieve == 0);
        EXPECT(spy->counts().list == 0);
        EXPECT(spy->counts().dump == 0);
        EXPECT(spy->counts().wipe == 0);
        EXPECT(spy->counts().purge == 0);
        EXPECT(spy->counts().stats == 0);
    }
}


CASE( "wipe_distributed_according_to_select" ) {

    // Build FDB from default config

    fdb5::FDB fdb(defaultConfig());
    EXPECT(ApiSpy::knownSpies().size() == 3);
    ApiSpy& spy_od(*ApiSpy::knownSpies()[0]);
    ApiSpy& spy_rd1(*ApiSpy::knownSpies()[1]);
    ApiSpy& spy_rd2(*ApiSpy::knownSpies()[2]);

    // Do some archiving

    fdb.wipe(fdb5::FDBToolRequest("class=od,expver=xxxx", false));

    EXPECT(spy_od.counts().wipe == 1);
    EXPECT(spy_rd1.counts().wipe == 0);
    EXPECT(spy_rd2.counts().wipe == 0);

    fdb.wipe(fdb5::FDBToolRequest("class=rd,expver=xxxx", false));

    EXPECT(spy_od.counts().wipe == 1);
    EXPECT(spy_rd1.counts().wipe == 1);
    EXPECT(spy_rd2.counts().wipe == 0);

    // Under specified - matches nothing. Requests halted at this point, as FDB retrieves need
    // to be fully specified

    fdb.wipe(fdb5::FDBToolRequest("class=rd,expver=zzzz", false));

    EXPECT(spy_od.counts().wipe == 1);
    EXPECT(spy_rd1.counts().wipe == 1);
    EXPECT(spy_rd2.counts().wipe == 0);

    //// Now match all the rd lanes

    fdb.wipe(fdb5::FDBToolRequest("class=rd", false));

    EXPECT(spy_od.counts().wipe == 1);
    EXPECT(spy_rd1.counts().wipe == 2);
    EXPECT(spy_rd2.counts().wipe == 1);

    // Explicitly match everything

    fdb.wipe(fdb5::FDBToolRequest("", true));

    EXPECT(spy_od.counts().wipe == 2);
    EXPECT(spy_rd1.counts().wipe == 3);
    EXPECT(spy_rd2.counts().wipe == 2);

    // And unused functions

    for (auto spy : {&spy_od, &spy_rd1, &spy_rd2}) {
        EXPECT(spy->counts().archive == 0);
        EXPECT(spy->counts().flush == 0);
        EXPECT(spy->counts().retrieve == 0);
        EXPECT(spy->counts().list == 0);
        EXPECT(spy->counts().dump == 0);
        EXPECT(spy->counts().where == 0);
        EXPECT(spy->counts().purge == 0);
        EXPECT(spy->counts().stats == 0);
    }
}


CASE( "purge_distributed_according_to_select" ) {

    // Build FDB from default config

    fdb5::FDB fdb(defaultConfig());
    EXPECT(ApiSpy::knownSpies().size() == 3);
    ApiSpy& spy_od(*ApiSpy::knownSpies()[0]);
    ApiSpy& spy_rd1(*ApiSpy::knownSpies()[1]);
    ApiSpy& spy_rd2(*ApiSpy::knownSpies()[2]);

    // Do some archiving

    fdb.purge(fdb5::FDBToolRequest("class=od,expver=xxxx", false));

    EXPECT(spy_od.counts().purge == 1);
    EXPECT(spy_rd1.counts().purge == 0);
    EXPECT(spy_rd2.counts().purge == 0);

    fdb.purge(fdb5::FDBToolRequest("class=rd,expver=xxxx", false));

    EXPECT(spy_od.counts().purge == 1);
    EXPECT(spy_rd1.counts().purge == 1);
    EXPECT(spy_rd2.counts().purge == 0);

    // Under specified - matches nothing. Requests halted at this point, as FDB retrieves need
    // to be fully specified

    fdb.purge(fdb5::FDBToolRequest("class=rd,expver=zzzz", false));

    EXPECT(spy_od.counts().purge == 1);
    EXPECT(spy_rd1.counts().purge == 1);
    EXPECT(spy_rd2.counts().purge == 0);

    //// Now match all the rd lanes

    fdb.purge(fdb5::FDBToolRequest("class=rd", false));

    EXPECT(spy_od.counts().purge == 1);
    EXPECT(spy_rd1.counts().purge == 2);
    EXPECT(spy_rd2.counts().purge == 1);

    // Explicitly match everything

    fdb.purge(fdb5::FDBToolRequest("", true));

    EXPECT(spy_od.counts().purge == 2);
    EXPECT(spy_rd1.counts().purge == 3);
    EXPECT(spy_rd2.counts().purge == 2);

    // And unused functions

    for (auto spy : {&spy_od, &spy_rd1, &spy_rd2}) {
        EXPECT(spy->counts().archive == 0);
        EXPECT(spy->counts().flush == 0);
        EXPECT(spy->counts().retrieve == 0);
        EXPECT(spy->counts().list == 0);
        EXPECT(spy->counts().dump == 0);
        EXPECT(spy->counts().where == 0);
        EXPECT(spy->counts().wipe == 0);
        EXPECT(spy->counts().stats == 0);
    }
}


CASE( "stats_distributed_according_to_select" ) {

    // Build FDB from default config

    fdb5::FDB fdb(defaultConfig());
    EXPECT(ApiSpy::knownSpies().size() == 3);
    ApiSpy& spy_od(*ApiSpy::knownSpies()[0]);
    ApiSpy& spy_rd1(*ApiSpy::knownSpies()[1]);
    ApiSpy& spy_rd2(*ApiSpy::knownSpies()[2]);

    // Do some archiving

    fdb.stats(fdb5::FDBToolRequest("class=od,expver=xxxx", false));

    EXPECT(spy_od.counts().stats == 1);
    EXPECT(spy_rd1.counts().stats == 0);
    EXPECT(spy_rd2.counts().stats == 0);

    fdb.stats(fdb5::FDBToolRequest("class=rd,expver=xxxx", false));

    EXPECT(spy_od.counts().stats == 1);
    EXPECT(spy_rd1.counts().stats == 1);
    EXPECT(spy_rd2.counts().stats == 0);

    // Under specified - matches nothing. Requests halted at this point, as FDB retrieves need
    // to be fully specified

    fdb.stats(fdb5::FDBToolRequest("class=rd,expver=zzzz", false));

    EXPECT(spy_od.counts().stats == 1);
    EXPECT(spy_rd1.counts().stats == 1);
    EXPECT(spy_rd2.counts().stats == 0);

    //// Now match all the rd lanes

    fdb.stats(fdb5::FDBToolRequest("class=rd", false));

    EXPECT(spy_od.counts().stats == 1);
    EXPECT(spy_rd1.counts().stats == 2);
    EXPECT(spy_rd2.counts().stats == 1);

    // Explicitly match everything

    fdb.stats(fdb5::FDBToolRequest("", true));

    EXPECT(spy_od.counts().stats == 2);
    EXPECT(spy_rd1.counts().stats == 3);
    EXPECT(spy_rd2.counts().stats == 2);

    // And unused functions

    for (auto spy : {&spy_od, &spy_rd1, &spy_rd2}) {
        EXPECT(spy->counts().archive == 0);
        EXPECT(spy->counts().flush == 0);
        EXPECT(spy->counts().retrieve == 0);
        EXPECT(spy->counts().list == 0);
        EXPECT(spy->counts().dump == 0);
        EXPECT(spy->counts().where == 0);
        EXPECT(spy->counts().wipe == 0);
        EXPECT(spy->counts().purge == 0);
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace test
}  // namespace fdb

int main(int argc, char **argv)
{
    return run_tests ( argc, argv );
}
