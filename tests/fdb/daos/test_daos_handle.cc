/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

// #include <cstdlib>
// #include <cstring>
// #include <iomanip>
// #include <unistd.h>
// #include <uuid/uuid.h>

#include "eckit/testing/Test.h"
#include "eckit/filesystem/URI.h"

//TODO: remove once pool_create is implemented
#include "dummy_daos.h"

#include "fdb5/daos/DaosCluster.h"
#include "fdb5/daos/DaosHandle.h"

using namespace eckit::testing;
using namespace eckit;

namespace fdb {
namespace test {

void deldir(eckit::PathName& p) {
    if (!p.exists()) {
        return;
    }

    std::vector<eckit::PathName> files;
    std::vector<eckit::PathName> dirs;
    p.children(files, dirs);

    for (auto& f : files) {
        f.unlink();
    }
    for (auto& d : dirs) {
        deldir(d);
    }

    p.rmdir();
}

CASE( "daos_handle" ) {

    std::string pool = "pool";

    // TODO: the following manual creation of a test pool is only valid for dummy daos.
    //       For this test to work with real daos, fdb5::DaosManager::create_pool(pool)
    //       should be implemented, which calls dmg_pool_create, declared in daos/tests_lib.h
    //       and defined in common/tests_dmg_helpers.c. It should be implemented in dummy daos
    //       as well, and remove the manual pool folder creation.
    // create a dummy daos pool manually
    PathName root = dummy_daos_root();
    PathName pool_path = root / pool;
    pool_path.mkdir();
    //fdb5::DaosManager::create_pool(pool);

    std::string cont = "cont";

    // create a new daos object, with an automatically allocated oid, and write 5 bytes into it

    fdb5::DaosHandle dh(pool, cont);

    char data[] = "test";

    dh.openForWrite(Length(0));
    long res = dh.write(data, (long) sizeof(data));

    // TODO: should position crash if unopened?
    EXPECT(dh.position() == Offset(sizeof(data)));

    dh.close();
    dh.flush();

    EXPECT(res == (long) sizeof(data));

    // reopen the same daos object and append 5 more bytes to it

    dh.openForAppend(Length(0));

    EXPECT(dh.position() == Offset(sizeof(data)));

    res = dh.write(data, (long) sizeof(data));

    EXPECT(dh.position() == Offset(2 * sizeof(data)));

    dh.close();
    dh.flush();

    std::string title = dh.title();

    // read the first 5 bytes of the object and check consistency

    fdb5::DaosHandle dh_read(title);

    Length t = dh_read.openForRead();

    EXPECT(dh_read.position() == Offset(0));
    EXPECT(t == Length(2 * sizeof(data)));

    char *data_read;

    data_read = (char *) malloc(sizeof(char) * ((size_t) sizeof(data)));

    res = dh_read.read(data_read, (long) sizeof(data));

    EXPECT(dh_read.position() == Offset(sizeof(data)));

    dh_read.close();

    EXPECT(res == (long) sizeof(data));
    EXPECT(std::memcmp(data, data_read, sizeof(data)) == 0);

    free(data_read);

    // other checks

    // Length l = dh.estimate();

    // attempt open for read of a non-existing object

    std::string oid = "0000000000000001.0000000000000000";
    fdb5::DaosHandle dh_fail(pool, cont, oid);
    bool caughtException = false;
    try {
        t = dh_fail.openForRead();
    } catch(eckit::Exception&) {
        caughtException = true;
    }
    EXPECT(caughtException);

    // open via URI

    eckit::URI uri("daos", eckit::PathName(title));
    fdb5::DaosHandle dh_uri(uri);
    t = dh_uri.openForRead();
    EXPECT(t == Length(2 * sizeof(data)));
    dh_uri.close();

    // TODO:
    //fdb5::DaosCluster::destroy_pool(pool);
    // destroy the container and pool manually
    deldir(pool_path);

}

}  // namespace test
}  // namespace fdb

int main(int argc, char **argv)
{
    return run_tests ( argc, argv );
}
