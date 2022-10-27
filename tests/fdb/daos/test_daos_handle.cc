/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <memory>

#include "eckit/testing/Test.h"
#include "eckit/filesystem/URI.h"
#include "eckit/filesystem/PathName.h"

#include "fdb5/daos/DaosSession.h"
#include "fdb5/daos/DaosPool.h"
#include "fdb5/daos/DaosContainer.h"
#include "fdb5/daos/DaosObject.h"
#include "fdb5/daos/DaosName.h"
#include "fdb5/daos/DaosHandle.h"

using namespace eckit::testing;
using namespace eckit;

namespace fdb {
namespace test {

// TODO: any way to catch exceptions and signals and destroy the pools as cleanup?
//       may be doable via test definition in cmake?
// TODO: remove folder created in daos_init()?

CASE( "daos_handle" ) {

    // TODO: most destructors have calls to DAOS. To be addressed.
    // TODO: Should e.g. DaosPool have an AutoClose?

    // TODO: currently, all pool and container connections are cached and kept open for the duration of the process. Would
    // be nice to close container connections as they become unused. However the DaosContainer instances are managed by the 
    // DaosPools/DaosSession, so we never know when the user has finished using a certain container. My current thought is
    // we don't need to fix this, as each process will only use a single pool and 2 * (indices involved) containers.
    // However in a large parallel application, while all client processes are running, there may be a large number of
    // open containers in the DAOS system. One idea would be to use shared pointers to count number of uses.



    // TODO: need to revisit use of references. 1st, check I'm not trying to reassign references anywhere. 2nd, rethink
    // API - user is now forced to use references.

    // TODO: cpp uuid wrapper, to avoid weird headers?

    // TODO: rule of three for classes with destructor?

    // TODO: change all pre-condition checks to ASSERTs
    // question: in the future ASSERTs will default to EcKit abortion. Not what we want in many pre-condition checks

    // TODO: use of uuid_generate_md5 can be removed completely

    // TODO: use scopes in short-lived tests?



    // TODO: there are issues in DaosPool::create

    // TODO: there are issues in DaosContainer::create

    // TODO: implement DaosContainer::destroy

    // TODO: implement missing methods in DaosName and DaosHandle



    // TODO: make DaosSession take configuration, and have some defaults if no config is provided
    fdb5::DaosSession s{};


    // UNNAMED POOL

    fdb5::DaosPool& unnamed_pool = s.createPool();
    // unnamed_pool.open();  // optional



    // NAMED POOL

    fdb5::DaosPool& pool2 = s.createPool(std::string("pool2"));
    // pool2.open();  // optional

    fdb5::DaosPool& pool3 = s.declarePool(std::string("pool2"));
    // pool3.open();  // optional

    EXPECT(&pool2 == &pool3);

    fdb5::DaosPool& pool4 = s.declarePool(std::string("pool4"));
    pool4.create();
    // pool4.open();  // optional



    // NAMED CONTAINER

    // // standalone create - NOT FOR NOW
    // fdb5::DaosContainer cont(pool, std::string("cont"));
    // // pool.open();  // optional
    // cont.create();
    // // cont.open();  // optional

    // // standalone declare - NOT FOR NOW
    // fdb5::DaosContainer cont2(pool, std::string("cont"));
    // // pool.open();  // optional
    // // cont.open();  // optional

    // pool2.open();  // optional
    fdb5::DaosContainer& cont2 = pool2.createContainer(std::string("cont2"));
    // cont2.open();  // optional

    // pool2.open();  // optional
    fdb5::DaosContainer& cont3 = pool2.declareContainer(std::string("cont2"));
    // cont3.open();  // optional

    EXPECT(&cont2 == &cont3);

    fdb5::DaosContainer& cont4 = pool2.declareContainer(std::string("cont4"));
    cont4.create();
    // cont4.open();  // optional



    // UNNAMED OBJECT

    // cont2.open();  // optional
    fdb5::DaosObject obj = cont2.createObject();
    // obj.open();  // optional



    // NAMED OBJECT

    std::string test_oid("0000000000000000.0000000000000000");

    fdb5::DaosObject obj2(cont2, test_oid);
    // cont2.open();  // optional
    obj2.create();
    // obj2.open();  // optional

    // cont2.open();  // optional
    fdb5::DaosObject obj4 = cont4.createObject(test_oid);
    // obj4.open();  // optional



    // NAME

    fdb5::DaosName n1("a", "b", "c.d");
    EXPECT(n1.asString() == "a:b:c.d");

    fdb5::DaosName n2("a:b:c.d");
    EXPECT(n2.asString() == "a:b:c.d");

    // TODO: correct way to construct generic URIs?
    eckit::URI u1("daos", eckit::PathName("a:b:c.d"));
    fdb5::DaosName n3(u1);
    EXPECT(n3.asString() == "a:b:c.d");
    EXPECT(n3.URI() == u1);

    fdb5::DaosName name = obj2.name();
    EXPECT(name.asString() == "pool2:cont2:" + test_oid);
    eckit::URI uri = name.URI();
    // TODO: implement asString for "daos" URIs
    // EXPECT(uri.asString() == "daos://pool:cont:" + test_oid);
    EXPECT(obj2.URI() == uri);

    // (serialise)

    // (deserialise)

    // alternative to deserialisation if name is known remotely:
    fdb5::DaosName deserialisedname(std::string("pool2"), std::string("cont2"), test_oid);
    
    deserialisedname.setSession(&s);
    std::cout << "Object size is: " << deserialisedname.size() << std::endl;

    fdb5::DaosObject readobj(s, deserialisedname);



    // DAOS HANDLE

    char data[] = "test";
    long res;

    fdb5::DaosHandle h(std::move(obj2));
    h.openForWrite(Length(sizeof(data)));
    {
        eckit::AutoClose closer(h);
        res = h.write(data, (long) sizeof(data));
        EXPECT(res == (long) sizeof(data));
        EXPECT(h.position() == Offset(sizeof(data)));
    }

    h.openForAppend(Length(sizeof(data)));
    {
        eckit::AutoClose closer(h);
        res = h.write(data, (long) sizeof(data));
        EXPECT(res == (long) sizeof(data));
        EXPECT(h.position() == Offset(2 * sizeof(data)));
    }

    h.flush();

    char read_data[10] = "";

    fdb5::DaosHandle h2(std::move(readobj));
    Length t = h2.openForRead();
    EXPECT(t == Length(2 * sizeof(data)));
    EXPECT(h2.position() == Offset(0));
    {
        eckit::AutoClose closer(h2);
        for (int i = 0; i < 2; ++i) {
            res = h2.read(read_data + i * sizeof(data), (long) sizeof(data));
            EXPECT(res == (long) sizeof(data));
        }
        EXPECT(h2.position() == Offset(2 * sizeof(data)));
    }

    EXPECT(std::memcmp(data, read_data, sizeof(data)) == 0);
    EXPECT(std::memcmp(data, read_data + sizeof(data), sizeof(data)) == 0);

    char read_data2[10] = "";

    deserialisedname.setSession(&s);
    std::unique_ptr<fdb5::DaosHandle> h3((fdb5::DaosHandle*) deserialisedname.dataHandle());
    h3->openForRead();
    {
        eckit::AutoClose closer(*h3);
        for (int i = 0; i < 2; ++i) {
            h3->read(read_data2 + i * sizeof(data), (long) sizeof(data));
        }
    }

    EXPECT(std::memcmp(data, read_data2, sizeof(data)) == 0);
    EXPECT(std::memcmp(data, read_data2 + sizeof(data), sizeof(data)) == 0);



    // TODO: POOL, CONTAINER AND OBJECT OPENING ARE OPTIONAL FOR DaosHandle::openForRead. Test it
    // TODO: CONTAINER AND OBJECT CREATION ARE OPTIONAL FOR DaosHandle::openForWrite. Test it
    // TODO: test unopen/uncreated DaosObject::size()


    // CLEANUP

    obj4.close();  // optional
    // obj4.destroy();  // NOTIMP

    //obj2.close();  // optional
    // obj2.destroy()  // lost ownership

    obj.close();  // optional
    // obj.destroy();  // NOTIMP

    cont4.close();  // optional
    // cont4.destroy();  // NOTIMP

    cont3.close();  // optional
    // cont3.destroy();  // NOTIMP

    cont2.close();  // optional
    // cont2.destroy();  // NOTIMP; should fail

    pool4.close();  // optional
    pool4.destroy();

    uuid_t pool3_uuid = {0};
    pool3.uuid(pool3_uuid);
    s.destroyPool(pool3_uuid);

    pool2.close();  // optional
    // pool2.destroy();  // this fails as the pool has already been destroyed in destruction of pool3

    unnamed_pool.close();  // optional
    unnamed_pool.destroy();



    // test DaosPool::name(), uuid(), label()

    // test pool destroy without close and ensure closed

    // test pool create from uuid

    // test pool declare form uuid

    // test construct DaosContainer with uuid

    // test DaosContainer::name()

    // test DaosObject::name()

    // test handle seek, skip, canSeek

    // test handle title()

    // test handle size and estimate

    // test DaosName::size()

    // test attempt open for read of a non-existing object

    // std::string oid = "0000000000000001.0000000000000000";
    // fdb5::DaosHandle dh_fail(pool.name(), cont, oid);
    // bool caughtException = false;
    // try {
    //     t = dh_fail.openForRead();
    // } catch(eckit::Exception&) {
    //     caughtException = true;
    // }
    // EXPECT(caughtException);

}

}  // namespace test
}  // namespace fdb

int main(int argc, char **argv)
{
    return run_tests ( argc, argv );
}
