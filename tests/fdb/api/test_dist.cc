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
#include "eckit/utils/Translator.h"

#include "fdb5/config/Config.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

#include "ApiSpy.h"

using namespace eckit::testing;
using namespace eckit;


namespace fdb {
namespace test {

//----------------------------------------------------------------------------------------------------------------------


fdb5::Config defaultConfig() {

    // Build a standard configuration to demonstrate features of DistFDB

    LocalConfiguration cfg_od;
    cfg_od.set("type", "spy");
    cfg_od.set("id", "1");

    LocalConfiguration cfg_rd1;
    cfg_rd1.set("type", "spy");
    cfg_rd1.set("id", "2");

    LocalConfiguration cfg_rd2;
    cfg_rd2.set("type", "spy");
    cfg_rd2.set("id", "3");

    fdb5::Config cfg;
    cfg.set("type", "dist");
    cfg.set("lanes", { cfg_od, cfg_rd1, cfg_rd2 });

    return cfg;
}


CASE( "archives_distributed_according_to_dist" ) {

    // Build FDB from default config

    fdb5::FDB fdb(defaultConfig());
    EXPECT(ApiSpy::knownSpies().size() == 3);
    ApiSpy& spy1(*ApiSpy::knownSpies()[0]);
    ApiSpy& spy2(*ApiSpy::knownSpies()[1]);
    ApiSpy& spy3(*ApiSpy::knownSpies()[2]);

    // Flush does nothing until dirty

    fdb.flush();

    EXPECT(spy1.counts().flush == 0);
    EXPECT(spy2.counts().flush == 0);
    EXPECT(spy3.counts().flush == 0);

    // Do some archiving

    const int nflush = 5;
    const int narch = 5;

    int flush_count = 0;

    for (int f = 0; f < nflush; f++) {
        for (int a = 0; a < narch; a++) {
            fdb5::Key k;
            k.set("class", "od");
            k.set("expver", "xxxx");
            k.set("f", eckit::Translator<int, std::string>()(f));
            k.set("a", eckit::Translator<int, std::string>()(a));

            fdb.archive(k, (void*)(100*f + a), 123);

            EXPECT((spy1.counts().archive + spy2.counts().archive + spy3.counts().archive) == (1 + a + (narch * f)));
            EXPECT(spy1.counts().flush + spy2.counts().flush + spy3.counts().flush == flush_count);
        }

        fdb.flush();
        EXPECT(spy1.counts().flush + spy2.counts().flush + spy3.counts().flush <= flush_count+3);
        flush_count = (spy1.counts().flush + spy2.counts().flush + spy3.counts().flush);
    }

    // And unused functions

    for (auto spy : {&spy1, &spy2, &spy3}) {
        EXPECT(spy->counts().retrieve == 0);
        EXPECT(spy->counts().list == 0);
        EXPECT(spy->counts().dump == 0);
        EXPECT(spy->counts().where == 0);
        EXPECT(spy->counts().wipe == 0);
        EXPECT(spy->counts().purge == 0);
        EXPECT(spy->counts().stats == 0);
    }
}


CASE( "retrieves_distributed_according_to_dist" ) {

    // Build FDB from default config

    fdb5::FDB fdb(defaultConfig());
    EXPECT(ApiSpy::knownSpies().size() == 3);
    ApiSpy& spy1(*ApiSpy::knownSpies()[0]);
    ApiSpy& spy2(*ApiSpy::knownSpies()[1]);
    ApiSpy& spy3(*ApiSpy::knownSpies()[2]);

    // Do some archiving

    MarsRequest req;
    req.setValues("class", std::vector<std::string>{"od"});
    req.setValues("expver", std::vector<std::string>{"xxxx"});
    fdb.retrieve(req);

    EXPECT(spy1.counts().retrieve == 1);
    EXPECT(spy2.counts().retrieve == 1);
    EXPECT(spy3.counts().retrieve == 1);

    req.setValues("class", std::vector<std::string>{std::string("rd")});
    fdb.retrieve(req);

    EXPECT(spy1.counts().retrieve == 2);
    EXPECT(spy2.counts().retrieve == 2);
    EXPECT(spy3.counts().retrieve == 2);

    // Under specified - matches nothing. Requests halted at this point, as FDB retrieves need
    // to be fully specified

    req.unsetValues("expver");
    fdb.retrieve(req);

    EXPECT(spy1.counts().retrieve == 3);
    EXPECT(spy2.counts().retrieve == 3);
    EXPECT(spy3.counts().retrieve == 3);

    // Now match all the rd lanes

    req.setValues("expver", std::vector<std::string>{"xx12", "yy21"});
    fdb.retrieve(req);

    EXPECT(spy1.counts().retrieve == 4);
    EXPECT(spy2.counts().retrieve == 4);
    EXPECT(spy3.counts().retrieve == 4);

    req.setValues("class", std::vector<std::string>{"od", "rd"});
    fdb.retrieve(req);

    EXPECT(spy1.counts().retrieve == 5);
    EXPECT(spy2.counts().retrieve == 5);
    EXPECT(spy3.counts().retrieve == 5);

    // And unused functions

    for (auto spy : {&spy1, &spy2, &spy3}) {
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

CASE( "lists_distributed_according_to_dist" ) {

    // Build FDB from default config

    fdb5::FDB fdb(defaultConfig());
    EXPECT(ApiSpy::knownSpies().size() == 3);
    ApiSpy& spy1(*ApiSpy::knownSpies()[0]);
    ApiSpy& spy2(*ApiSpy::knownSpies()[1]);
    ApiSpy& spy3(*ApiSpy::knownSpies()[2]);

    // Do some archiving

    fdb.list(fdb5::FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0]);

    EXPECT(spy1.counts().list == 1);
    EXPECT(spy2.counts().list == 1);
    EXPECT(spy3.counts().list == 1);

    fdb.list(fdb5::FDBToolRequest::requestsFromString("class=rd,expver=xxxx")[0]);

    EXPECT(spy1.counts().list == 2);
    EXPECT(spy2.counts().list == 2);
    EXPECT(spy3.counts().list == 2);

    // Under specified - matches nothing. Requests halted at this point, as FDB retrieves need
    // to be fully specified

    fdb.list(fdb5::FDBToolRequest::requestsFromString("class=rd,expver=zzzz")[0]);

    EXPECT(spy1.counts().list == 3);
    EXPECT(spy2.counts().list == 3);
    EXPECT(spy3.counts().list == 3);

    //// Now match all the rd lanes

    fdb.list(fdb5::FDBToolRequest::requestsFromString("class=rd")[0]);

    EXPECT(spy1.counts().list == 4);
    EXPECT(spy2.counts().list == 4);
    EXPECT(spy3.counts().list == 4);

    // Explicitly match everything

    fdb.list(fdb5::FDBToolRequest({}, true));

    EXPECT(spy1.counts().list == 5);
    EXPECT(spy2.counts().list == 5);
    EXPECT(spy3.counts().list == 5);

    // And unused functions

    for (auto spy : {&spy1, &spy2, &spy3}) {
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


CASE( "dump_distributed_according_to_dist" ) {

    // Build FDB from default config

    fdb5::FDB fdb(defaultConfig());
    EXPECT(ApiSpy::knownSpies().size() == 3);
    ApiSpy& spy1(*ApiSpy::knownSpies()[0]);
    ApiSpy& spy2(*ApiSpy::knownSpies()[1]);
    ApiSpy& spy3(*ApiSpy::knownSpies()[2]);

    // Do some archiving

    fdb.dump(fdb5::FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0]);

    EXPECT(spy1.counts().dump == 1);
    EXPECT(spy2.counts().dump == 1);
    EXPECT(spy3.counts().dump == 1);

    fdb.dump(fdb5::FDBToolRequest::requestsFromString("class=rd,expver=xxxx")[0]);

    EXPECT(spy1.counts().dump == 2);
    EXPECT(spy2.counts().dump == 2);
    EXPECT(spy3.counts().dump == 2);

    // Under specified - matches nothing. Requests halted at this point, as FDB retrieves need
    // to be fully specified

    fdb.dump(fdb5::FDBToolRequest::requestsFromString("class=rd,expver=zzzz")[0]);

    EXPECT(spy1.counts().dump == 3);
    EXPECT(spy2.counts().dump == 3);
    EXPECT(spy3.counts().dump == 3);

    //// Now match all the rd lanes

    fdb.dump(fdb5::FDBToolRequest::requestsFromString("class=rd")[0]);

    EXPECT(spy1.counts().dump == 4);
    EXPECT(spy2.counts().dump == 4);
    EXPECT(spy3.counts().dump == 4);

    // Explicitly match everything

    fdb.dump(fdb5::FDBToolRequest({}, true));

    EXPECT(spy1.counts().dump == 5);
    EXPECT(spy2.counts().dump == 5);
    EXPECT(spy3.counts().dump == 5);

    // And unused functions

    for (auto spy : {&spy1, &spy2, &spy3}) {
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

CASE( "where_distributed_according_to_dist" ) {

    // Build FDB from default config

    fdb5::FDB fdb(defaultConfig());
    EXPECT(ApiSpy::knownSpies().size() == 3);
    ApiSpy& spy1(*ApiSpy::knownSpies()[0]);
    ApiSpy& spy2(*ApiSpy::knownSpies()[1]);
    ApiSpy& spy3(*ApiSpy::knownSpies()[2]);

    // Do some archiving

    fdb.where(fdb5::FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0]);

    EXPECT(spy1.counts().where == 1);
    EXPECT(spy2.counts().where == 1);
    EXPECT(spy3.counts().where == 1);

    fdb.where(fdb5::FDBToolRequest::requestsFromString("class=rd,expver=xxxx")[0]);

    EXPECT(spy1.counts().where == 2);
    EXPECT(spy2.counts().where == 2);
    EXPECT(spy3.counts().where == 2);

    // Under specified - matches nothing. Requests halted at this point, as FDB retrieves need
    // to be fully specified

    fdb.where(fdb5::FDBToolRequest::requestsFromString("class=rd,expver=zzzz")[0]);

    EXPECT(spy1.counts().where == 3);
    EXPECT(spy2.counts().where == 3);
    EXPECT(spy3.counts().where == 3);

    //// Now match all the rd lanes

    fdb.where(fdb5::FDBToolRequest::requestsFromString("class=rd")[0]);

    EXPECT(spy1.counts().where == 4);
    EXPECT(spy2.counts().where == 4);
    EXPECT(spy3.counts().where == 4);

    // Explicitly match everything

    fdb.where(fdb5::FDBToolRequest({}, true));

    EXPECT(spy1.counts().where == 5);
    EXPECT(spy2.counts().where == 5);
    EXPECT(spy3.counts().where == 5);

    // And unused functions

    for (auto spy : {&spy1, &spy2, &spy3}) {
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


CASE( "wipe_distributed_according_to_dist" ) {

    // Build FDB from default config

    fdb5::FDB fdb(defaultConfig());
    EXPECT(ApiSpy::knownSpies().size() == 3);
    ApiSpy& spy1(*ApiSpy::knownSpies()[0]);
    ApiSpy& spy2(*ApiSpy::knownSpies()[1]);
    ApiSpy& spy3(*ApiSpy::knownSpies()[2]);

    // Do some archiving

    fdb.wipe(fdb5::FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0]);

    EXPECT(spy1.counts().wipe == 1);
    EXPECT(spy2.counts().wipe == 1);
    EXPECT(spy3.counts().wipe == 1);

    fdb.wipe(fdb5::FDBToolRequest::requestsFromString("class=rd,expver=xxxx")[0]);

    EXPECT(spy1.counts().wipe == 2);
    EXPECT(spy2.counts().wipe == 2);
    EXPECT(spy3.counts().wipe == 2);

    // Under specified - matches nothing. Requests halted at this point, as FDB retrieves need
    // to be fully specified

    fdb.wipe(fdb5::FDBToolRequest::requestsFromString("class=rd,expver=zzzz")[0]);

    EXPECT(spy1.counts().wipe == 3);
    EXPECT(spy2.counts().wipe == 3);
    EXPECT(spy3.counts().wipe == 3);

    //// Now match all the rd lanes

    fdb.wipe(fdb5::FDBToolRequest::requestsFromString("class=rd")[0]);

    EXPECT(spy1.counts().wipe == 4);
    EXPECT(spy2.counts().wipe == 4);
    EXPECT(spy3.counts().wipe == 4);

    // Explicitly match everything

    fdb.wipe(fdb5::FDBToolRequest({}, true));

    EXPECT(spy1.counts().wipe == 5);
    EXPECT(spy2.counts().wipe == 5);
    EXPECT(spy3.counts().wipe == 5);

    // And unused functions

    for (auto spy : {&spy1, &spy2, &spy3}) {
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


CASE( "purge_distributed_according_to_dist" ) {

    // Build FDB from default config

    fdb5::FDB fdb(defaultConfig());
    EXPECT(ApiSpy::knownSpies().size() == 3);
    ApiSpy& spy1(*ApiSpy::knownSpies()[0]);
    ApiSpy& spy2(*ApiSpy::knownSpies()[1]);
    ApiSpy& spy3(*ApiSpy::knownSpies()[2]);

    // Do some archiving

    fdb.purge(fdb5::FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0]);

    EXPECT(spy1.counts().purge == 1);
    EXPECT(spy2.counts().purge == 1);
    EXPECT(spy3.counts().purge == 1);

    fdb.purge(fdb5::FDBToolRequest::requestsFromString("class=rd,expver=xxxx")[0]);

    EXPECT(spy1.counts().purge == 2);
    EXPECT(spy2.counts().purge == 2);
    EXPECT(spy3.counts().purge == 2);

    // Under specified - matches nothing. Requests halted at this point, as FDB retrieves need
    // to be fully specified

    fdb.purge(fdb5::FDBToolRequest::requestsFromString("class=rd,expver=zzzz")[0]);

    EXPECT(spy1.counts().purge == 3);
    EXPECT(spy2.counts().purge == 3);
    EXPECT(spy3.counts().purge == 3);

    //// Now match all the rd lanes

    fdb.purge(fdb5::FDBToolRequest::requestsFromString("class=rd")[0]);

    EXPECT(spy1.counts().purge == 4);
    EXPECT(spy2.counts().purge == 4);
    EXPECT(spy3.counts().purge == 4);

    // Explicitly match everything

    fdb.purge(fdb5::FDBToolRequest({}, true));

    EXPECT(spy1.counts().purge == 5);
    EXPECT(spy2.counts().purge == 5);
    EXPECT(spy3.counts().purge == 5);

    // And unused functions

    for (auto spy : {&spy1, &spy2, &spy3}) {
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


CASE( "stats_distributed_according_to_dist" ) {

    // Build FDB from default config

    fdb5::FDB fdb(defaultConfig());
    EXPECT(ApiSpy::knownSpies().size() == 3);
    ApiSpy& spy1(*ApiSpy::knownSpies()[0]);
    ApiSpy& spy2(*ApiSpy::knownSpies()[1]);
    ApiSpy& spy3(*ApiSpy::knownSpies()[2]);

    // Do some archiving

    fdb.stats(fdb5::FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0]);

    EXPECT(spy1.counts().stats == 1);
    EXPECT(spy2.counts().stats == 1);
    EXPECT(spy3.counts().stats == 1);

    fdb.stats(fdb5::FDBToolRequest::requestsFromString("class=rd,expver=xxxx")[0]);

    EXPECT(spy1.counts().stats == 2);
    EXPECT(spy2.counts().stats == 2);
    EXPECT(spy3.counts().stats == 2);

    // Under specified - matches nothing. Requests halted at this point, as FDB retrieves need
    // to be fully specified

    fdb.stats(fdb5::FDBToolRequest::requestsFromString("class=rd,expver=zzzz")[0]);

    EXPECT(spy1.counts().stats == 3);
    EXPECT(spy2.counts().stats == 3);
    EXPECT(spy3.counts().stats == 3);

    //// Now match all the rd lanes

    fdb.stats(fdb5::FDBToolRequest::requestsFromString("class=rd")[0]);

    EXPECT(spy1.counts().stats == 4);
    EXPECT(spy2.counts().stats == 4);
    EXPECT(spy3.counts().stats == 4);

    // Explicitly match everything

    fdb.stats(fdb5::FDBToolRequest({}, true));

    EXPECT(spy1.counts().stats == 5);
    EXPECT(spy2.counts().stats == 5);
    EXPECT(spy3.counts().stats == 5);

    // And unused functions

    for (auto spy : {&spy1, &spy2, &spy3}) {
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
