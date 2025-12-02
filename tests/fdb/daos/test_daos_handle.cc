/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <cstring>
#include <memory>

#include "eckit/config/Resource.h"
#include "eckit/config/YAMLConfiguration.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/TmpDir.h"
#include "eckit/filesystem/TmpFile.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/FileHandle.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/testing/Test.h"

#include "fdb5/fdb5_config.h"

#include "fdb5/daos/DaosArrayHandle.h"
#include "fdb5/daos/DaosContainer.h"
#include "fdb5/daos/DaosException.h"
#include "fdb5/daos/DaosName.h"
#include "fdb5/daos/DaosObject.h"
#include "fdb5/daos/DaosPool.h"
#include "fdb5/daos/DaosSession.h"

using namespace eckit::testing;
using namespace eckit;

#ifdef fdb5_HAVE_DUMMY_DAOS
eckit::TmpDir& tmp_dummy_daos_root() {
    static eckit::TmpDir d{};
    return d;
}
#endif

bool endsWith(const std::string& str, const std::string& end) {
    return 0 == str.compare(str.length() - end.length(), end.length(), end);
}

namespace fdb {
namespace test {

#ifdef fdb5_HAVE_DUMMY_DAOS
CASE("Setup") {

    tmp_dummy_daos_root().mkdir();
    ::setenv("DUMMY_DAOS_DATA_ROOT", tmp_dummy_daos_root().path().c_str(), 1);
}
#endif

#ifdef fdb5_HAVE_DAOS_ADMIN
CASE("DaosPool") {

    /// @todo: currently, all pool and container connections are cached and kept open for the duration of the process.
    /// Would
    ///   be nice to close container connections as they become unused. However the DaosContainer instances are managed
    ///   by the DaosPools/DaosSession, so we never know when the user has finished using a certain container. My
    ///   current thought is we don't need to fix this, as each process will only use a single pool and 2 * (indices
    ///   involved) containers. However in a large parallel application, while all client processes are running, there
    ///   may be a large number of open containers in the DAOS system. One idea would be to use shared pointers to count
    ///   number of uses.

    /// @todo: A declarative approach would be better in my opinion.
    ///   The current approach is an imperative one, where DaosObject and DaosContainer instances always represent
    ///   existing entities in DAOS from the instant they are created. In highly parallel workflows, validity of such
    ///   instances will be ephemeral, and by the time we perform an action on them, the DAOS entity they represent may
    ///   no longer exist. In the declarative approach, the containers and objects would be opened right before the
    ///   action and fail if they don't exist. In the imperative approach they would fail as well, but the initial
    ///   checks performed to ensure existence of the DAOS entities would be useless and degrade performance.

    /// @todo: cpp uuid wrapper, to avoid weird headers

    /// @todo: do not return iterators in e.g. DaosSession::getCachedPool. Return DaosPool&

    /// @todo: replace deque by map

    /// @note: using hard-coded config defaults in DaosManager
    fdb5::DaosSession s{};

    SECTION("unnamed pool") {

        fdb5::DaosPool& pool = s.createPool();  /// admin function, not usually called in the client code
        fdb5::AutoPoolDestroy destroyer(pool);

        std::cout << pool.name() << std::endl;

        EXPECT(pool.name().size() == 36);

        /// @todo: there's an attempt to close unopened pool here due to the pool destruction mechanism
    }

    SECTION("named pool") {

        std::string pool_name{"test_pool_1"};

        fdb5::DaosPool& pool = s.createPool(pool_name);
        fdb5::AutoPoolDestroy destroyer(pool);

        std::cout << pool.name() << std::endl;

        EXPECT(pool.name() == pool_name);

        fdb5::DaosPool& pool_h = s.getPool(pool_name);

        EXPECT(&pool == &pool_h);
    }

    SECTION("pool uuid actions") {

        fdb5::DaosPool& pool = s.createPool();
        fdb5::AutoPoolDestroy destroyer(pool);

        const fdb5::UUID& pool_uuid = pool.uuid();

        char uuid_cstr[37];
        uuid_unparse(pool_uuid.internal, uuid_cstr);
        std::cout << uuid_cstr << std::endl;
    }

    /// @todo: test passing some session ad-hoc config for DAOS client

    /// @todo: there's an extra pair of daos_init and daos_fini happening here
}
#endif

CASE("DaosContainer, DaosArray and DaosKeyValue") {

    std::string cont_name{"test_cont_2"};

    /// again using hard-coded config defaults in DaosManager
    fdb5::DaosSession s{};

#ifdef fdb5_HAVE_DAOS_ADMIN
    std::string pool_name{"test_pool_2"};
    fdb5::DaosPool& pool = s.createPool(pool_name);
    fdb5::AutoPoolDestroy destroyer(pool);
    fdb5::DaosContainer& cont = pool.createContainer(cont_name);
#else
#ifdef fdb5_HAVE_DUMMY_DAOS
    std::string pool_uuid{"00000000-0000-0000-0000-000000000001"};
    std::string pool_name{"test_pool_2"};
    (tmp_dummy_daos_root() / pool_uuid).mkdir();
    ::symlink((tmp_dummy_daos_root() / pool_uuid).path().c_str(), (tmp_dummy_daos_root() / pool_name).path().c_str());
#else
    std::string pool_name;
    pool_name = eckit::Resource<std::string>("fdbDaosTestPool;$FDB_DAOS_TEST_POOL", pool_name);
    EXPECT(pool_name.length() > 0);
#endif
    fdb5::DaosPool& pool = s.getPool(pool_name);
    fdb5::DaosContainer& cont = pool.createContainer(cont_name);
    fdb5::AutoContainerDestroy destroyer(cont);
#endif

    SECTION("named container") {

        std::cout << cont.name() << std::endl;

        EXPECT(cont.name() == cont_name);

        fdb5::DaosContainer& cont_h = pool.getContainer(cont_name);

        EXPECT(&cont == &cont_h);

        EXPECT_THROWS_AS(pool.getContainer("nonexisting_cont"), fdb5::DaosEntityNotFoundException);

        std::vector<std::string> cont_list(pool.listContainers());
        /// @note: not compatible with parallel test runs on a same pool (if !have_FDB_DAOS_ADMIN)
        /// EXPECT(cont_list.size() == 1);
        /// EXPECT(cont_list.front() == cont_name);

        /// @todo: two attempts to close unopened containers here. This is due to mechanism triggered upon
        ///   pool destroy to ensure all matching container handles in the session cache are closed.
        ///   One way to address this would be to have a flag expectOpen = true in DaosContainer::close().
        ///   Whenever close() is called as part of pool destroy or batch container close, a false value
        ///   could be passed to the close() method to avoid logging a warning message.
    }

    SECTION("unnamed object") {

        // create new object with new automatically allocated oid
        fdb5::DaosArray arr = cont.createArray();
        std::cout << "New automatically allocated Array OID: " << arr.name() << std::endl;
        fdb5::DaosKeyValue kv = cont.createKeyValue();
        std::cout << "New automatically allocated KeyValue OID: " << kv.name() << std::endl;

        /// @todo:
        // arr.destroy();
        // kv.destroy();
    }

    SECTION("DaosOID and named object") {

        // create new object with oid generated from user input
        uint32_t hi = 0x00000001;
        uint64_t lo = 0x0000000000000001;
        fdb5::DaosOID oid{hi, lo, DAOS_OT_ARRAY, OC_S1};
        fdb5::DaosArray write_obj = cont.createArray(oid);

        std::string id_string = write_obj.name();
        std::cout << "New user-spec-based OID: " << id_string << std::endl;
        EXPECT(id_string.length() == 32);
        /// @todo: do these checks numerically. Also test invalid characters, etc.
        std::string end{"000000010000000000000001"};
        EXPECT(0 == id_string.compare(id_string.length() - end.length(), end.length(), end));

        // represent existing object with known oid
        fdb5::DaosOID read_id{id_string};
        fdb5::DaosArray read_obj{cont, read_id};

        // attempt access non-existing object
        fdb5::DaosArrayOID oid_ne{0, 0, OC_S1};
        oid_ne.generateReservedBits(cont);
        EXPECT_THROWS_AS(fdb5::DaosArray obj(cont, oid_ne), fdb5::DaosEntityNotFoundException);

        /// @todo:
        // write_obj.destroy();
    }

    /// @todo: DaosArray write and read

    SECTION("DaosKeyValue put and get") {

        fdb5::DaosKeyValue kv = cont.createKeyValue();

        std::string test_key{"test_key_1"};

        char data[] = "test_data_1";
        kv.put(test_key, data, (uint64_t)sizeof(data));

        uint64_t size = kv.size(test_key);
        EXPECT(size == (uint64_t)sizeof(data));

        uint64_t res;
        char read_data[20] = "";
        res = kv.get(test_key, read_data, sizeof(read_data));
        EXPECT(res == size);
        EXPECT(std::memcmp(data, read_data, sizeof(data)) == 0);

        EXPECT(!kv.has("nonexisting_key"));
        EXPECT(kv.size("nonexisting_key") == 0);
        EXPECT_THROWS_AS(kv.get("nonexisting_key", nullptr, 0), fdb5::DaosEntityNotFoundException);

        /// @todo:
        // kv.destroy();
    }

    SECTION("DaosName, DaosArrayName, DaosKeyValueName") {

        /// @todo: is the private ctor of DaosArray and DaosKeyValue kept private?

        fdb5::DaosName np{pool_name};
        EXPECT(np.exists());
        EXPECT(np.URI().asString() == std::string("daos:") + pool_name);

        fdb5::DaosName np_ne{"a"};
        EXPECT_NOT(np_ne.exists());

        fdb5::DaosName nc{pool_name, cont_name};
        EXPECT(nc.exists());
        EXPECT(nc.URI().asString() == std::string("daos:") + pool_name + "/" + cont_name);

        fdb5::DaosName nc_ne{"a", "b"};
        EXPECT_NOT(nc_ne.exists());

        fdb5::DaosOID test_oid{1, 2, DAOS_OT_ARRAY, OC_S1};
        fdb5::DaosName n1{pool_name, cont_name, test_oid};
        EXPECT(n1.poolName() == pool_name);
        EXPECT(n1.hasContainerName());
        EXPECT(n1.containerName() == cont_name);
        EXPECT(n1.hasOID());
        EXPECT_NOT(test_oid.wasGenerated());
        fdb5::DaosOID test_oid_gen = n1.OID();
        EXPECT(test_oid_gen.wasGenerated());
        EXPECT_NOT(n1.exists());
        n1.create();
        EXPECT(n1.exists());
        std::string start{std::string("daos:") + pool_name + "/" + cont_name + "/"};
        std::string end{"000000010000000000000002"};
        std::string n1_uri_str = n1.URI().asString();
        EXPECT(0 == n1_uri_str.compare(0, start.length(), start));
        EXPECT(0 == n1_uri_str.compare(n1_uri_str.length() - end.length(), end.length(), end));
        EXPECT(n1.size() == eckit::Length(0));

        fdb5::DaosName nc_new{pool_name, "new_cont"};
        EXPECT_NOT(nc_new.exists());
        fdb5::DaosArrayName na_new = nc_new.createArrayName();
        EXPECT(nc_new.exists());
        EXPECT_NOT(na_new.exists());
        na_new.create();
        EXPECT(na_new.exists());
        /// @todo:
        // na_new.destroy();
        // EXPECT_NOT(na_new.exists());
        // EXPECT(nc_new.exists());
        nc_new.destroy();
        EXPECT_NOT(nc_new.exists());

        std::string test_oid_2_str{"00000000000000020000000000000002"};
        fdb5::DaosOID test_oid_2{test_oid_2_str};
        fdb5::DaosName n2("a", "b", test_oid_2);
        EXPECT(n2.asString() == "a/b/" + test_oid_2_str);

        eckit::URI u2("daos", "a/b/" + test_oid_2_str);
        fdb5::DaosName n3(u2);
        EXPECT(n3.asString() == "a/b/" + test_oid_2_str);
        EXPECT(n3.URI() == u2);

        fdb5::DaosArray arr{cont, n1.OID()};
        fdb5::DaosName n4{arr};
        EXPECT(n4.exists());

        eckit::URI uri = n1.URI();
        EXPECT(arr.URI() == uri);

        /// @todo:
        // n1.destroy();
        // EXPECT_NOT(n1.exists());
        // EXPECT_NOT(n4.exists());

        fdb5::DaosKeyValueName nkv = nc.createKeyValueName();
        /// @todo: currently, existence of kvs is checked by attempting open.
        ///   But a DAOS kv open creates it if not exists. Should implement a better kv
        ///   existency check not involving open/create before uncommenting the following test.
        // EXPECT_NOT(nkv.exists());
        nkv.create();
        EXPECT(nkv.exists());
        fdb5::DaosKeyValue kv{s, nkv};
        char data[] = "test_value_3";
        kv.put("test_key_3", data, (uint64_t)sizeof(data));
        EXPECT(nkv.has("test_key_3"));
        /// @todo:
        // nkv.destroy();
        // EXPECT_NOT(nkv.exists());

        /// @todo: serialise

        /// @todo: deserialise
        fdb5::DaosName deserialisedname(pool_name, cont_name, n1.OID());

        std::cout << "Object size is: " << deserialisedname.size() << std::endl;

        kv.put("test_key_4", data, sizeof(data));
        EXPECT(nkv.has("test_key_4"));
        std::vector<std::string> keys = kv.keys();
        EXPECT(keys.size() == 2);
        if (keys[0] == "test_key_3") {
            EXPECT(keys[1] == "test_key_4");
        }
        else {
            EXPECT(keys[0] == "test_key_4");
            EXPECT(keys[1] == "test_key_3");
        }
    }

    SECTION("DaosHandle write, append and read") {

        fdb5::DaosOID test_oid{1, 3, DAOS_OT_ARRAY, OC_S1};
        fdb5::DaosArrayName test_name{pool_name, cont_name, test_oid};

        char data[] = "test_data_2";
        long res;

        fdb5::DaosArrayHandle h{test_name};
        /// @todo: this triggers array create but does not wipe existing array if any
        h.openForWrite(Length(sizeof(data)));
        {
            eckit::AutoClose closer(h);
            res = h.write(data, (uint64_t)sizeof(data));
            EXPECT(res == (uint64_t)sizeof(data));
            EXPECT(h.position() == Offset(sizeof(data)));

            res = h.write(data, (uint64_t)sizeof(data));
            EXPECT(res == (uint64_t)sizeof(data));
            EXPECT(h.position() == Offset(2 * sizeof(data)));
        }

        h.flush();

        eckit::URI u{std::string("daos:") + pool_name + "/" + cont_name + "/" + test_name.OID().asString()};
        fdb5::DaosArrayName read_name{u};

        fdb5::DaosArrayHandle h2(read_name);
        Length t = h2.openForRead();
        EXPECT(t == Length(2 * sizeof(data)));
        EXPECT(h2.position() == Offset(0));
        std::vector<char> read_data((size_t)t, 0);
        {
            eckit::AutoClose closer(h2);
            for (int i = 0; i < 2; ++i) {
                res = h2.read(&read_data[0] + i * sizeof(data), (uint64_t)sizeof(data));
                EXPECT(res == (uint64_t)sizeof(data));
            }
            EXPECT(h2.position() == Offset(2 * sizeof(data)));
        }
        EXPECT(std::memcmp(data, &read_data[0], sizeof(data)) == 0);
        EXPECT(std::memcmp(data, &read_data[0] + sizeof(data), sizeof(data)) == 0);

        std::unique_ptr<eckit::DataHandle> h3(read_name.dataHandle());
        Length t2 = h3->openForRead();
        std::vector<char> read_data2((size_t)t2, 0);
        {
            eckit::AutoClose closer(*h3);
            for (int i = 0; i < 2; ++i) {
                h3->read(&read_data2[0] + i * sizeof(data), (uint64_t)sizeof(data));
            }
        }
        EXPECT(std::memcmp(data, &read_data2[0], sizeof(data)) == 0);
        EXPECT(std::memcmp(data, &read_data2[0] + sizeof(data), sizeof(data)) == 0);

        fdb5::DaosArrayHandle dh_fail(
            fdb5::DaosArrayName(pool_name, cont_name, fdb5::DaosOID{1, 0, DAOS_OT_ARRAY, OC_S1}));
        EXPECT_THROWS_AS(dh_fail.openForRead(), fdb5::DaosEntityNotFoundException);

        /// @todo: pool, container and object opening are optional before calling
        ///   DaosHandle::openForRead. Test it.
        /// @todo: container and object creation are optional before calling
        ///   DaosHandle::openForWrite. Test it.
        /// @todo: test unopen/uncreated DaosObject::size()
    }

    /// @note: There's no actual need for container destruction as either pool or
    ///   container are autodestroyed depending on if_HAVE_DAOS_ADMIN.

    /// @todo: when enabling this, AutoPoolDestroy fails as a corresponding DaosContainer instance
    ///   still exists in the DaosPool, but the corresponding container does not exist and
    ///   dummy_daos destroy fails trying to rename an unexisting directory. The issue of pool
    ///   and container destruction and invalidation needs to be addressed first.
    // cont.destroy();

    /// @todo: test open pool destroy ensure it is closed
}

/// @todo: test a new case where some DAOS operations are carried out with a DaosSession with specific config
///   overriding (but not rewriting) DaosManager defaults

CASE("DaosName and DaosHandle workflows") {

    std::string cont_name{"test_cont_3"};

    /// @note: again using hard-coded config defaults in DaosManager
    fdb5::DaosSession s{};

#ifdef fdb5_HAVE_DAOS_ADMIN
    std::string pool_name{"test_pool_3"};
    fdb5::DaosPool& pool = s.createPool(pool_name);
    fdb5::AutoPoolDestroy destroyer(pool);
    fdb5::DaosContainer& cont = pool.createContainer(cont_name);
#else
#ifdef fdb5_HAVE_DUMMY_DAOS
    std::string pool_uuid{"00000000-0000-0000-0000-000000000002"};
    std::string pool_name{"test_pool_3"};
    (tmp_dummy_daos_root() / pool_uuid).mkdir();
    ::symlink((tmp_dummy_daos_root() / pool_uuid).path().c_str(), (tmp_dummy_daos_root() / pool_name).path().c_str());
#else
    std::string pool_name;
    pool_name = eckit::Resource<std::string>("fdbDaosTestPool;$FDB_DAOS_TEST_POOL", pool_name);
    EXPECT(pool_name.length() > 0);
#endif
    fdb5::DaosPool& pool = s.getPool(pool_name);
    fdb5::DaosContainer& cont = pool.createContainer(cont_name);
    fdb5::AutoContainerDestroy destroyer(cont);
#endif

    char data[] = "test_data_3";
    eckit::Length len{sizeof(data)};

    long res;

    SECTION("Array write to existing pool and container, with ungenerated OID") {

        fdb5::DaosArrayOID oid{3, 1, OC_S1};
        fdb5::DaosArrayName na{pool_name, cont_name, oid};
        std::unique_ptr<eckit::DataHandle> h(na.dataHandle());
        /// @todo: cast to reference, and dereference ptr with *
        EXPECT(dynamic_cast<fdb5::DaosArrayHandle*>(h.get()));
        h->openForWrite(len);
        {
            eckit::AutoClose closer(*h);
            h->write(data, len);
        }
        EXPECT_THROWS_AS(oid.asString(), eckit::AssertionFailed);  /// ungenerated
        std::string oid_end{"000000030000000000000001"};
        EXPECT(endsWith(na.OID().asString(), oid_end));
        EXPECT(na.size() == len);
    }

    SECTION("Array write to existing pool and container, with automatically generated OID") {

        fdb5::DaosName n{pool_name, cont_name};
        fdb5::DaosArrayName na = n.createArrayName();
        std::unique_ptr<eckit::DataHandle> h(na.dataHandle());
        h->openForWrite(len);
        {
            eckit::AutoClose closer(*h);
            h->write(data, len);
        }
        std::cout << "Generated OID: " << na.OID().asString() << std::endl;
        EXPECT(na.size() == len);
    }

    SECTION("Array write to existing pool and container, with URI") {

        eckit::URI container{std::string("daos:") + pool_name + "/" + cont_name};
        fdb5::DaosName n{container};
        fdb5::DaosArrayName na = n.createArrayName();
        std::unique_ptr<eckit::DataHandle> h(na.dataHandle());
        h->openForWrite(len);
        {
            eckit::AutoClose closer(*h);
            h->write(data, len);
        }
        std::cout << "Generated OID: " << na.OID().asString() << std::endl;
        EXPECT(na.size() == len);
    }

    SECTION("Array write/read to/from existing pool and container, with generated OID") {

        /// write
        fdb5::DaosArrayOID oid{3, 2, OC_S1};
        oid.generateReservedBits(cont);
        fdb5::DaosArrayName na{pool_name, cont_name, oid};
        // fdb5::DaosArrayName n{pool_name, cont_name, {"00110000000000000000000000000001"}};
        std::unique_ptr<eckit::DataHandle> h(na.dataHandle());
        h->openForWrite(len);  /// @todo: creates it if not exists, should it fail?
        {
            eckit::AutoClose closer(*h);
            h->write(data, len);
        }
        std::string oid_end{"000000030000000000000002"};
        EXPECT(endsWith(na.OID().asString(), oid_end));
        EXPECT(na.size() == len);
    }

    SECTION("Array write to existing pool but non-existing container, with ungenerated OID") {

        fdb5::DaosArrayOID oid{3, 3, OC_S1};
        fdb5::DaosArrayName na{pool_name, "new_cont_1", oid};
        EXPECT_NOT(fdb5::DaosName(pool_name, "new_cont_1").exists());
        std::unique_ptr<eckit::DataHandle> h(na.dataHandle());  /// @todo: should dataHandle() assert pool exists?
        h->openForWrite(len);
        fdb5::AutoContainerDestroy d(pool.getContainer("new_cont_1"));
        {
            eckit::AutoClose closer(*h);
            h->write(data, len);
        }
        EXPECT_THROWS_AS(oid.asString(), eckit::AssertionFailed);  /// ungenerated
        std::string oid_end{"000000030000000000000003"};
        EXPECT(endsWith(na.OID().asString(), oid_end));
        EXPECT(na.size() == len);
        EXPECT(fdb5::DaosName(pool_name, "new_cont_1").exists());
    }

    SECTION("Array write to existing pool but non-existing container, with automatically generated OID") {

        fdb5::DaosName n{pool_name, "new_cont_2"};
        fdb5::DaosArrayName na = n.createArrayName();
        EXPECT(n.exists());
        EXPECT_NOT(na.exists());
        std::unique_ptr<eckit::DataHandle> h(na.dataHandle());
        h->openForWrite(len);
        fdb5::AutoContainerDestroy d(pool.getContainer("new_cont_2"));
        {
            eckit::AutoClose closer(*h);
            h->write(data, len);
        }
        std::cout << "Generated OID: " << na.OID().asString() << std::endl;
        EXPECT(na.size() == len);
        EXPECT(fdb5::DaosName(pool_name, "new_cont_2").exists());
    }

    /// @todo: should use of a non-existing container plus generated OID be forbidden?
    ///   In principle it should be forbidden, as an OID can only be generated via existing enclosing container.
    ///   However it would not be straightforward to implement with the curren design of DaosOID and DaosName.
    ///   For now it is allowed. The container and object will be created in openForWrite() if not exist.
    SECTION("Array write to existing pool but non-existing container, with generated OID") {

        fdb5::DaosName nc{pool_name, "new_cont_3"};
        fdb5::DaosArrayName na = nc.createArrayName(OC_S1);
        nc.destroy();
        EXPECT_NOT(nc.exists());
        std::unique_ptr<eckit::DataHandle> h(na.dataHandle());
        h->openForWrite(len);
        fdb5::AutoContainerDestroy d(pool.getContainer("new_cont_3"));
        {
            eckit::AutoClose closer(*h);
            h->write(data, len);
        }
        EXPECT(nc.exists());
        EXPECT(na.size() == len);
    }

    SECTION("Array read from existing pool and container, with generated OID") {

        fdb5::DaosArrayOID oid{3, 4, OC_S1};
        fdb5::DaosArrayName na{pool_name, cont_name, oid};
        std::unique_ptr<eckit::DataHandle> h(na.dataHandle());
        h->openForWrite(len);
        {
            eckit::AutoClose closer(*h);
            h->write(data, len);
        }
        EXPECT(na.size() == len);

        /// existing generated OID
        fdb5::DaosArrayName na_read{pool_name, cont_name, na.OID()};
        std::unique_ptr<eckit::DataHandle> h2(na_read.dataHandle());
        Length t = h2->openForRead();
        std::vector<char> read_data((size_t)t, 0);
        {
            eckit::AutoClose closer(*h2);
            EXPECT(eckit::Length(read_data.size()) >= h2->size());
            res = h2->read(&read_data[0], h2->size());
        }
        EXPECT(res == len);
        EXPECT(std::memcmp(data, &read_data[0], sizeof(data)) == 0);

        /// non-existing generated OID
        fdb5::DaosArrayOID oid_ne{3, 5, OC_S1};
        oid_ne.generateReservedBits(cont);
        fdb5::DaosArrayName na_read_ne{pool_name, cont_name, oid_ne};
        std::unique_ptr<eckit::DataHandle> h3(na_read_ne.dataHandle());
        EXPECT_THROWS_AS(h3->openForRead(), fdb5::DaosEntityNotFoundException);
    }

    SECTION("Array read from existing pool and container, with generated OID, reading a range") {

        fdb5::DaosArrayOID oid{3, 6, OC_S1};
        fdb5::DaosArrayName na{pool_name, cont_name, oid};
        std::unique_ptr<eckit::DataHandle> h(na.dataHandle());
        h->openForWrite(len);
        {
            eckit::AutoClose closer(*h);
            h->write(data, len);
        }
        EXPECT(na.size() == len);

        /// existing generated OID
        fdb5::DaosArrayName na_read{pool_name, cont_name, na.OID()};
        std::unique_ptr<eckit::DataHandle> h2(na_read.dataHandle());
        long skip_bytes = 10;
        Length t = h2->openForRead();
        std::vector<char> read_data((size_t)(t - eckit::Length(skip_bytes)), 0);
        {
            eckit::AutoClose closer(*h2);
            EXPECT(h2->size() >= skip_bytes);
            EXPECT(eckit::Length(read_data.size()) >= (t - eckit::Length(skip_bytes)));
            std::memset(&read_data[0], 0, read_data.size());
            h2->seek(skip_bytes);
            res = h2->read(&read_data[0], t - eckit::Length(skip_bytes));
        }
        EXPECT(res == len - eckit::Length(skip_bytes));
        EXPECT(std::memcmp(data + skip_bytes, &read_data[0], sizeof(data) - skip_bytes) == 0);
    }

    SECTION("Array read from existing pool and container, with ungenerated OID") {

        fdb5::DaosArrayOID oid{3, 7, OC_S1};
        fdb5::DaosArrayName na{pool_name, cont_name, oid};
        std::unique_ptr<eckit::DataHandle> h(na.dataHandle());
        h->openForWrite(len);
        {
            eckit::AutoClose closer(*h);
            h->write(data, len);
        }
        EXPECT(na.size() == len);

        /// existing ungenerated OID
        fdb5::DaosArrayName na_read{pool_name, cont_name, oid};
        std::unique_ptr<eckit::DataHandle> h2(na_read.dataHandle());
        Length t = h2->openForRead();
        std::vector<char> read_data((size_t)t, 0);
        {
            eckit::AutoClose closer(*h2);
            EXPECT(eckit::Length(read_data.size()) >= h2->size());
            std::memset(&read_data[0], 0, read_data.size());
            res = h2->read(&read_data[0], h2->size());
        }
        EXPECT(res == len);
        EXPECT(std::memcmp(data, &read_data[0], sizeof(data)) == 0);

        /// non-existing ungenerated OID
        fdb5::DaosArrayOID oid_ne{3, 8, OC_S1};
        fdb5::DaosArrayName na_read_ne{pool_name, cont_name, oid_ne};
        std::unique_ptr<eckit::DataHandle> h3(na_read_ne.dataHandle());
        EXPECT_THROWS_AS(h3->openForRead(), fdb5::DaosEntityNotFoundException);
    }

    SECTION("Array read from existing pool and non-existing container") {

        fdb5::DaosArrayOID oid{3, 9, OC_S1};
        fdb5::DaosArrayName na{pool_name, "new_cont_4", oid};
        std::unique_ptr<eckit::DataHandle> h(na.dataHandle());
        EXPECT_THROWS_AS(h->openForRead(), fdb5::DaosEntityNotFoundException);
        EXPECT_NOT(fdb5::DaosName(pool_name, "new_cont_4").exists());
    }

    /// @todo: test analogous KeyValue workflows
}

}  // namespace test
}  // namespace fdb

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
