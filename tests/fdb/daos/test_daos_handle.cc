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
// TODO: ensure test folder is clean

CASE( "DAOS POOL" ) {

    // TODO: cross-section DaosSession is destroyed before some per-section instances

    // TODO: review approach taken in destructors. Most do non-throwing calls to DAOS.
    //       Most do potentially throwing eckit::Log. DaosHandle has AutoClose.

    // TODO: currently, all pool and container connections are cached and kept open for the duration of the process. Would
    // be nice to close container connections as they become unused. However the DaosContainer instances are managed by the 
    // DaosPools/DaosSession, so we never know when the user has finished using a certain container. My current thought is
    // we don't need to fix this, as each process will only use a single pool and 2 * (indices involved) containers.
    // However in a large parallel application, while all client processes are running, there may be a large number of
    // open containers in the DAOS system. One idea would be to use shared pointers to count number of uses.

    // TODO: A declarative approach would be better in my opinion.
    // The current approach is an imperative one, where DaosObject and DaosContainer instances always represent existing entities in DAOS from the instant they are created.
    // In highly parallel workflows, validity of such instances will be ephemeral, and by the time we perform an action on them, the DAOS entity they represent may
    // no longer exist. In the declarative approach, the containers and objects would be opened right before the action and fail if they don't exist. In the imperative
    // approach they would fail as well, but the initial checks performed to ensure existence of the DAOS entities would be useless and degrade performance.



    // TODO: review DAOS calls in test output

    // notes in my first note

    // notes in notes for simon

    // comments in chat with simon

    // TODO: do not return iterators in e.g. DaosSession::getCachedPool

    // TODO: issues in DaosPool::create and destroy

    // TODO: properly implement DaosPool::exists(), DaosContainer::exists(), DaosObject::exists()

    // TODO: issues in DaosContainer::create and destroy

    // TODO: solve question on default constructor of DaosOID

    // TODO: should daos_size_t be exposed as eckit::Length to user?

    // TODO: think about DaosName::dataHandle overwrite parameter

    // TODO: DaosHandle serialisation

    // TODO: small TODOs in DaosHandle

    // TODO: use of static_assert? where?

    // TODO: use scopes in short-lived tests?

    // TODO: change all pre-condition checks to ASSERTs
    // question: in the future ASSERTs will default to EcKit abortion. Not what we want in many pre-condition checks

    // TODO: rule of three for classes with destructor?

    // TODO: implement missing methods in DaosName and DaosHandle

    // TODO: cpp uuid wrapper, to avoid weird headers?

    // TODO: use of uuid_generate_md5 can be removed completely




    // TODO: make DaosSession take configuration, and have some defaults if no config is provided
    fdb5::DaosSession s{};

    SECTION("UNNAMED POOL") {

        fdb5::DaosPool& pool = s.createPool();  // admin function, not usually called in the client code.

        std::cout << pool.name() << std::endl;

        EXPECT(pool.name().size() == 36);

        // TODO: there's an attempt to close unopened pool here
        pool.destroy();  // admin

    }

    SECTION("NAMED POOL") {

        fdb5::DaosPool& pool = s.createPool("pool");

        std::cout << pool.name() << std::endl;

        EXPECT(pool.name() == "pool");

        fdb5::DaosPool& pool_h = s.getPool("pool");

        EXPECT(&pool == &pool_h);

        pool.destroy();

    }

    SECTION("POOL UUID ACTIONS") {

        fdb5::DaosPool& pool = s.createPool();

        uuid_t pool_uuid = {0};
        pool.uuid(pool_uuid);

        char uuid_cstr[37];
        uuid_unparse(pool_uuid, uuid_cstr);
        std::cout << uuid_cstr << std::endl;

        pool.destroy();

    }

    // TODO: there's an extra pair of daos_init and daos_fini happening here

}

CASE( "DAOS HANDLE" ) {

    fdb5::DaosSession s{};

    fdb5::DaosPool& pool = s.createPool("pool");

    fdb5::DaosContainer& cont = pool.createContainer("cont");

    SECTION("NAMED CONTAINER") {

        std::cout << cont.name() << std::endl;

        EXPECT(cont.name() == "cont");

        fdb5::DaosContainer& cont_h = pool.getContainer("cont");

        EXPECT(&cont == &cont_h);

        // TODO: test container by uuid

        // TODO
        /// @note if opening the container is the only way of checking it is valid, ask developers
        //EXPECT_THROWS(pool.getContainer("cont2"), fdb5::DaosException);

        // TODO: two attempts to close unopened containers here

    }

    SECTION("UNNAMED OBJECT") {

        // TODO: there's an extra daos_cont_create being issued here

        // create new object with new automatically allocated oid
        fdb5::DaosObject obj = cont.createObject();
        std::cout << "New automatically allocated OID: " << obj.name() << std::endl; 

        // TODO
        //obj.destroy();

        // TODO: there's an extra attempt to close the container here

    }

    SECTION("NAMED OBJECT") {

        // create new object with oid generated from user input
        uint32_t hi = 0x00000001;
        uint64_t lo = 0x0000000000000002;
        fdb5::DaosObject write_obj = cont.createObject(hi, lo);

        std::string id_string = write_obj.name();
        std::cout << "New user-spec-based OID: " << id_string << std::endl;
        EXPECT(id_string.length() == 32);
        std::string end{"000000010000000000000002"};
        EXPECT(0 == id_string.compare(id_string.length() - end.length(), end.length(), end));

        // represent existing object with known oid
        fdb5::DaosOID read_id{id_string};
        fdb5::DaosObject read_obj{cont, read_id};
        
        // TODO: validate existence in constructor
        // EXPECT_THROWS(DaosObject obj(cont, 0, 0));

        // TODO
        //write_obj.destroy();

    }

    SECTION("DAOS NAME") {

        std::string test_oid_str{"00000000000000010000000000000002"};
        fdb5::DaosOID test_oid{test_oid_str};

        fdb5::DaosName n1("a", "b", test_oid);
        EXPECT(n1.asString() == "a:b:" + test_oid_str);

        fdb5::DaosName n2("a:b:" + test_oid_str);
        EXPECT(n2.asString() == "a:b:" + test_oid_str);

        eckit::URI u1("daos", "a:b:" + test_oid_str);
        fdb5::DaosName n3(u1);
        EXPECT(n3.asString() == "a:b:" + test_oid_str);
        EXPECT(n3.URI() == u1);
        
        uint32_t hi = 0x00000001;
        uint64_t lo = 0x0000000000000002;
        fdb5::DaosObject obj = cont.createObject(hi, lo);
        fdb5::DaosName name{obj};

        std::string name_str = name.asString();
        std::string start{"pool:cont:"};
        std::string end{"000000010000000000000002"};
        EXPECT(0 == name_str.compare(0, start.length(), start));
        EXPECT(0 == name_str.compare(name_str.length() - end.length(), end.length(), end));

        eckit::URI uri = name.URI();
        // TODO: implement asString for "daos" URIs
        // EXPECT(uri.asString() == "daos://pool:cont:" + test_oid);
        EXPECT(obj.URI() == uri);

        // TODO: test name.exists and others

        // TODO: serialise

        // TODO: deserialise
        fdb5::DaosName deserialisedname(std::string("pool"), std::string("cont"), test_oid);
    
        deserialisedname.setSession(&s);
        std::cout << "Object size is: " << deserialisedname.size() << std::endl;

        // TODO
        //obj.destroy();

    }

    SECTION("DAOS HANDLE") {

        uint32_t hi = 0x00000001;
        uint64_t lo = 0x0000000000000002;
        fdb5::DaosObject obj = cont.createObject(hi, lo);
        std::string test_oid_str{"00000000000000010000000000000002"};
        fdb5::DaosOID test_oid{test_oid_str};
        fdb5::DaosName deserialisedname(std::string("pool"), std::string("cont"), test_oid);
        deserialisedname.setSession(&s);
        fdb5::DaosObject readobj(s, deserialisedname);

        // TODO: isn't openForWrite / Append re-creating already existing objects? (they must exist if instantiated)

        char data[] = "test";
        long res;

        fdb5::DaosHandle h(std::move(obj));
        // TODO: this triggers array create if needed (not here) but does not wipe existing array if any
        h.openForWrite(Length(sizeof(data)));
        {
            eckit::AutoClose closer(h);
            res = h.write(data, (long) sizeof(data));
            EXPECT(res == (long) sizeof(data));
            EXPECT(h.position() == Offset(sizeof(data)));
        }

        // TODO: this triggers array create again...
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

        // lost ownership of obj, recreate and destroy
        // fdb5::DaosObject obj_rm{cont, test_oid};
        // obj_rm.destroy();  // NOTIMP

    }

    // TODO
    // cont.destroy();  // NOTIMP

    pool.destroy();


    

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

// *write to new object with automatic or defined oid
//     DaosSession s{???}
//     p = s.createPool()
//     c = p.ensureCont("c")  // p.createCont("c")
//     o = c.createObject()  // c.createObject(hi32, lo64)
//     uri = o.URI()
//     DataHandle dh(std::move(o))
//     dh.openForWrite()
//     {
//         AutoClose closer(dh);
//         dh.write(data, len);
//     }


// should allow write to new object from DaosName/URI?
// should enable creation of target object in DaosHandle::openForWrite/Append if it comes from DaosName/URI/fully specified DaosOID?
// in principle we said openForWrite/Append should create if not exists


// write to existing object with known OID
//     DaosSession s{???}
//     p = s.getPool("p")
//     c = p.getCont("c")
//     DaosObject o{c, DaosOID(hi64, lo64)}
//     uri = o.URI()
//     DataHandle dh(std::move(o))
//     dh.openForWrite()
//     {
//         AutoClose closer(dh);
//         dh.write(data, len);
//     }


// write to existing object from DaosName/URI
//     DaosSession s{???}
//     DaosName n{uri}
//     n.setSession(s)
//     dh = n.dataHandle(offset, len)
//     dh->openForWrite()
//     {
//         AutoClose closer(*dh)
//         dh.write(data, len)
//     }






// read from existing object with known OID
//     DaosSession s{???}
//     p = s.getPool("p")
//     c = p.getCont("c")
//     DaosObject o{c, DaosOID(hi64, lo64)}
//     uri = o.URI()
//     DataHandle dh(std::move(o))
//     dh.openForRead()
//     {
//         AutoClose closer(dh);
//         dh.read(data, len);
//     }


// *read from existing object from DaosName/URI
//     DaosSession s{???}  // not in DaosFieldLocation
//     DaosName n{uri}
//     n.setSession(s)  // Problem in DaosFieldLocation!!!
//     dh = n.dataHandle(offset, len)
//     dh->openForRead()  // not in DaosFieldLocation
//     {
//         AutoClose closer(*dh)
//         dh.read(data, len)
//     }