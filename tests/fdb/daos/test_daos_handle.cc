/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/testing/Test.h"
#include "eckit/filesystem/URI.h"

#include "fdb5/daos/DaosCluster.h"
#include "fdb5/daos/DaosHandle.h"
#include "fdb5/daos/DaosPool.h"

using namespace eckit::testing;
using namespace eckit;

namespace fdb {
namespace test {

// TODO: any way to catch exceptions and signals and destroy the pools as cleanup?
//       may be doable via test definition in cmake?
CASE( "daos_handle" ) {

    fdb5::DaosPool pool(std::string("pool"));
    pool.create();

    std::string cont = "cont";

    // create a new daos object, with an automatically allocated oid, and write 5 bytes into it

    fdb5::DaosObject obj(pool.name(), cont);
    fdb5::DaosHandle* dh = obj.daosHandle();

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
    fdb5::DaosHandle dh_fail(pool.name(), cont, oid);
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

    pool.destroy();

}

}  // namespace test
}  // namespace fdb

int main(int argc, char **argv)
{
    return run_tests ( argc, argv );
}
