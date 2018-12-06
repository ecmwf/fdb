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

#include "ApiSpy.h"

using namespace eckit::testing;
using namespace eckit;


namespace fdb {
namespace test {

//----------------------------------------------------------------------------------------------------------------------


CASE( "archives_distributed_according_to_select" ) {

    // Build configuration

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

    // Build FDB from config

    fdb5::FDB fdb(cfg);
    EXPECT(ApiSpy::knownSpies().size() == 3);
    ApiSpy& spy_od(*ApiSpy::knownSpies()[0]);
    ApiSpy& spy_rd1(*ApiSpy::knownSpies()[1]);
    ApiSpy& spy_rd2(*ApiSpy::knownSpies()[2]);

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
}


//----------------------------------------------------------------------------------------------------------------------

}  // namespace test
}  // namespace fdb

int main(int argc, char **argv)
{
    return run_tests ( argc, argv );
}
