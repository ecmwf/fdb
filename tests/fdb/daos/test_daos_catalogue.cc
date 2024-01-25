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
#include "eckit/testing/Test.h"
#include "eckit/filesystem/URI.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/TmpFile.h"
#include "eckit/filesystem/TmpDir.h"
#include "eckit/io/FileHandle.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/config/YAMLConfiguration.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/fdb5_config.h"
#include "fdb5/config/Config.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

#include "fdb5/toc/TocStore.h"

#include "fdb5/daos/DaosSession.h"
#include "fdb5/daos/DaosPool.h"
#include "fdb5/daos/DaosArrayPartHandle.h"

#include "fdb5/daos/DaosStore.h"
#include "fdb5/daos/DaosFieldLocation.h"
#include "fdb5/daos/DaosCatalogueWriter.h"
#include "fdb5/daos/DaosCatalogueReader.h"

using namespace eckit::testing;
using namespace eckit;

static void deldir(eckit::PathName& p) {
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
};


#ifdef fdb5_HAVE_DUMMY_DAOS
eckit::TmpDir& tmp_dummy_daos_root() {
    static eckit::TmpDir d{};
    return d;
}
#endif

// temporary schema,spaces,root files common to all DAOS Catalogue tests

eckit::TmpFile& schema_file() {
    static eckit::TmpFile f{};
    return f;
}

eckit::TmpFile& spaces_file() {
    static eckit::TmpFile f{};
    return f;
}

eckit::TmpFile& roots_file() {
    static eckit::TmpFile f{};
    return f;
}

eckit::PathName& catalogue_tests_tmp_root() {
    static eckit::PathName cd("./daos_catalogue_tests_fdb_root");
    return cd;
}

namespace fdb {
namespace test {

CASE( "Setup" ) {

#ifdef fdb5_HAVE_DUMMY_DAOS
    tmp_dummy_daos_root().mkdir();
    ::setenv("DUMMY_DAOS_DATA_ROOT", tmp_dummy_daos_root().path().c_str(), 1);
#endif

    // ensure fdb root directory exists. If not, then that root is 
    // registered as non existing and Catalogue/Store tests fail.
    if (catalogue_tests_tmp_root().exists()) deldir(catalogue_tests_tmp_root());
    catalogue_tests_tmp_root().mkdir();
    ::setenv("FDB_ROOT_DIRECTORY", catalogue_tests_tmp_root().path().c_str(), 1);

    // prepare schema for tests involving DaosCatalogue

    std::string schema_str{"[ a, b [ c, d [ e, f ]]]"};

    std::unique_ptr<eckit::DataHandle> hs(schema_file().fileHandle());
    hs->openForWrite(schema_str.size());
    {
        eckit::AutoClose closer(*hs);
        hs->write(schema_str.data(), schema_str.size());
    }

    // this is necessary to avoid ~fdb/etc/fdb/schema being used where
    // LibFdb5::instance().defaultConfig().schema() is called
    // due to no specified schema file (e.g. in Key::registry())
    ::setenv("FDB_SCHEMA_FILE", schema_file().path().c_str(), 1);

    // prepare scpaces

    std::string spaces_str{".* all Default"};

    std::unique_ptr<eckit::DataHandle> hsp(spaces_file().fileHandle());
    hsp->openForWrite(spaces_str.size());
    {
        eckit::AutoClose closer(*hsp);
        hsp->write(spaces_str.data(), spaces_str.size());
    }

    ::setenv("FDB_SPACES_FILE", spaces_file().path().c_str(), 1);

    // prepare roots

    std::string roots_str{catalogue_tests_tmp_root().asString() + " all yes yes"};

    std::unique_ptr<eckit::DataHandle> hr(roots_file().fileHandle());
    hr->openForWrite(roots_str.size());
    {
        eckit::AutoClose closer(*hr);
        hr->write(roots_str.data(), roots_str.size());
    }

    ::setenv("FDB_ROOTS_FILE", roots_file().path().c_str(), 1);

}

CASE("DaosCatalogue tests") {

    // test parameters

    std::string root_cont_name{"test_root"};
    int container_oids_per_alloc = 1000;
#ifdef fdb5_HAVE_DAOS_ADMIN
    std::string pool_name{"fdb_pool2"};
#else
    std::string pool_name;
    pool_name = eckit::Resource<std::string>(
        "fdbDaosTestPool;$FDB_DAOS_TEST_POOL", pool_name
    );
    EXPECT(pool_name.length() > 0);
#endif

    // bootstrap daos

    uuid_t pool_uuid = {0};
    {
        /// @btodo: should DaosManager really be configured here? May be better not to configure it
        ///         here, only specify client config in FDB config file, and let the constructor of 
        ///         DaosStore or DaosCatalogue configure DaosManager.
        fdb5::DaosManager::instance().configure(
            eckit::LocalConfiguration(YAMLConfiguration(
                "container_oids_per_alloc: " + std::to_string(container_oids_per_alloc)
            ))
        );
        fdb5::DaosSession s{};
#ifdef fdb5_HAVE_DAOS_ADMIN
        fdb5::DaosPool& pool = s.createPool(pool_name);
#else
        fdb5::DaosPool& pool = s.getPool(pool_name);
#endif
        pool.uuid(pool_uuid);
    }

    SECTION("DaosCatalogue archive (index) and retrieve without a Store") {

        std::string config_str{
            "schema : " + schema_file().path() + "\n"
            "daos:\n"
            "  catalogue:\n"
            "    pool: " + pool_name + "\n"
            "    root_cont: " + root_cont_name + "\n"
            "  client:\n"
            "    container_oids_per_alloc: " + std::to_string(container_oids_per_alloc)
        };

        fdb5::Config config{YAMLConfiguration(config_str)};

        /// @note: a=11,b=22 instead of a=1,b=2 to avoid collision with potential parallel runs of store tests using a=1,b=2
        fdb5::Key request_key{"a=11,b=22,c=3,d=4,e=5,f=6"};
        fdb5::Key db_key{"a=11,b=22"};
        fdb5::Key index_key{"c=3,d=4"};
        fdb5::Key field_key{"e=5,f=6"};

        // archive

        /// DaosManager is configured with client config from the file
        std::unique_ptr<fdb5::FieldLocation> loc(new fdb5::DaosFieldLocation(
            eckit::URI{"daos", "test_uri"}, eckit::Offset(0), eckit::Length(1), fdb5::Key()
        ));

        {
            fdb5::DaosCatalogueWriter dcatw{db_key, config};

            fdb5::DaosName db_cont{pool_name, db_key.valuesToString()};
            fdb5::DaosKeyValueOID cat_kv_oid{0, 0, OC_S1};  /// @todo: take oclass from config
            fdb5::DaosKeyValueName cat_kv{pool_name, db_key.valuesToString(), cat_kv_oid};
            EXPECT(db_cont.exists());
            EXPECT(cat_kv.exists());

            fdb5::Catalogue& cat = dcatw;
            cat.selectIndex(index_key);
            fdb5::DaosKeyValueOID index_kv_oid{index_key.valuesToString(), OC_S1};  /// @todo: take oclass from config
            fdb5::DaosKeyValueName index_kv{pool_name, db_key.valuesToString(), index_kv_oid};
            EXPECT(index_kv.exists());
            EXPECT(cat_kv.has(index_key.valuesToString()));

            fdb5::CatalogueWriter& catw = dcatw;
            catw.archive(field_key, std::move(loc));
            EXPECT(index_kv.has(field_key.valuesToString()));
            fdb5::DaosKeyValueOID e_axis_kv_oid{index_key.valuesToString() + std::string{".e"}, OC_S1};
            fdb5::DaosKeyValueName e_axis_kv{pool_name, db_key.valuesToString(), e_axis_kv_oid};
            EXPECT(e_axis_kv.exists());
            EXPECT(e_axis_kv.has("5"));
            fdb5::DaosKeyValueOID f_axis_kv_oid{index_key.valuesToString() + std::string{".f"}, OC_S1};
            fdb5::DaosKeyValueName f_axis_kv{pool_name, db_key.valuesToString(), f_axis_kv_oid};
            EXPECT(f_axis_kv.exists());
            EXPECT(f_axis_kv.has("6"));
        }

        // retrieve

        {
            fdb5::DaosCatalogueReader dcatr{db_key, config};

            fdb5::Catalogue& cat = dcatr;
            cat.selectIndex(index_key);

            fdb5::Field f;
            fdb5::CatalogueReader& catr = dcatr;
            catr.retrieve(field_key, f);
            EXPECT(f.location().uri().name() == eckit::URI("daos", "test_uri").name());
            EXPECT(f.location().offset() == eckit::Offset(0));
            EXPECT(f.location().length() == eckit::Length(1));
        }

        // remove (manual deindex)

        {
            fdb5::DaosCatalogueWriter dcatw{db_key, config};
            fdb5::DaosName db_cont{dcatw.uri()};
            std::ostream out(std::cout.rdbuf());

            fdb5::DaosCatalogue::remove(db_cont, out, out, true);

            fdb5::DaosKeyValueOID cat_kv_oid{0, 0, OC_S1};  /// @todo: take oclass from config
            fdb5::DaosKeyValueName cat_kv{pool_name, db_key.valuesToString(), cat_kv_oid};
            EXPECT_NOT(cat_kv.exists());
            EXPECT_NOT(db_cont.exists());
        }

    }

    SECTION("DaosCatalogue archive (index) and retrieve with a DaosStore") {

        // FDB configuration

        std::string config_str{
            "schema : " + schema_file().path() + "\n"
            "daos:\n"
            "  store:\n"
            "    pool: " + pool_name + "\n"
            "  catalogue:\n"
            "    pool: " + pool_name + "\n"
            "    root_cont: " + root_cont_name + "\n"
            "  client:\n"
            "    container_oids_per_alloc: " + std::to_string(container_oids_per_alloc)
        };

        fdb5::Config config{YAMLConfiguration(config_str)};

        // schema

        fdb5::Schema schema{schema_file()};

        // request

        fdb5::Key request_key{"a=11,b=22,c=3,d=4,e=5,f=6"};
        fdb5::Key db_key{"a=11,b=22"};
        fdb5::Key index_key{"c=3,d=4"};
        fdb5::Key field_key{"e=5,f=6"};

        // store data

        char data[] = "test";

        fdb5::DaosStore dstore{schema, db_key, config};
        fdb5::Store& store = static_cast<fdb5::Store&>(dstore);
        std::unique_ptr<fdb5::FieldLocation> loc(store.archive(index_key, data, sizeof(data)));
        /// @todo: there are two cont create with label here
        /// @todo: again, daos_fini happening before cont and pool close

        // index data

        {
            fdb5::DaosCatalogueWriter dcatw{db_key, config};
            fdb5::Catalogue& cat = dcatw;
            cat.deselectIndex();
            cat.selectIndex(index_key);
            fdb5::CatalogueWriter& catw = dcatw;
            catw.archive(field_key, std::move(loc));

            /// flush store before flushing catalogue
            dstore.flush();  // not necessary if using a DAOS store
        }

        // find data

        fdb5::Field field;
        {
            fdb5::DaosCatalogueReader dcatr{db_key, config};
            fdb5::Catalogue& cat = dcatr;
            cat.selectIndex(index_key);
            fdb5::CatalogueReader& catr = dcatr;
            catr.retrieve(field_key, field);
        }
        std::cout << "Read location: " << field.location() << std::endl;

        // retrieve data

        std::unique_ptr<eckit::DataHandle> dh(store.retrieve(field));
        EXPECT(dynamic_cast<fdb5::DaosArrayPartHandle*>(dh.get()));
    
        eckit::MemoryHandle mh;
        dh->copyTo(mh);
        EXPECT(mh.size() == eckit::Length(sizeof(data)));
        EXPECT(::memcmp(mh.data(), data, sizeof(data)) == 0);

        // deindex data

        {
            fdb5::DaosCatalogueWriter dcat{db_key, config};
            fdb5::Catalogue& cat = static_cast<fdb5::Catalogue&>(dcat);
            std::ostream out(std::cout.rdbuf());
            metkit::mars::MarsRequest r = db_key.request("retrieve");
            std::unique_ptr<fdb5::WipeVisitor> wv(cat.wipeVisitor(store, r, out, true, false, false));
            cat.visitEntries(*wv, store, false);
        }

        /// @todo: again, daos_fini happening before

    }

    SECTION("DaosCatalogue archive (index) and retrieve with a TocStore") {

        // FDB configuration

        std::string config_str{
            "schema : " + schema_file().path() + "\n"
            "daos:\n"
            "  catalogue:\n"
            "    pool: " + pool_name + "\n"
            "    root_cont: " + root_cont_name + "\n"
            "  client:\n"
            "    container_oids_per_alloc: " + std::to_string(container_oids_per_alloc)
        };

        fdb5::Config config{YAMLConfiguration(config_str)};

        // schema

        fdb5::Schema schema{schema_file()};

        // request

        fdb5::Key request_key{"a=11,b=22,c=3,d=4,e=5,f=6"};
        fdb5::Key db_key{"a=11,b=22"};
        fdb5::Key index_key{"c=3,d=4"};
        fdb5::Key field_key{"e=5,f=6"};

        // store data

        char data[] = "test";

        fdb5::TocStore tstore{schema, db_key, config};
        fdb5::Store& store = static_cast<fdb5::Store&>(tstore);
        std::unique_ptr<fdb5::FieldLocation> loc(store.archive(index_key, data, sizeof(data)));
        /// @todo: there are two cont create with label here
        /// @todo: again, daos_fini happening before cont and pool close

        // index data

        {
            fdb5::DaosCatalogueWriter dcatw{db_key, config};
            fdb5::Catalogue& cat = dcatw;
            cat.deselectIndex();
            cat.selectIndex(index_key);
            fdb5::CatalogueWriter& catw = dcatw;
            catw.archive(field_key, std::move(loc));

            /// flush store before flushing catalogue
            tstore.flush();
        }

        // find data

        fdb5::Field field;
        {
            fdb5::DaosCatalogueReader dcatr{db_key, config};
            fdb5::Catalogue& cat = dcatr;
            cat.selectIndex(index_key);
            fdb5::CatalogueReader& catr = dcatr;
            catr.retrieve(field_key, field);
        }
        std::cout << "Read location: " << field.location() << std::endl;

        // retrieve data

        std::unique_ptr<eckit::DataHandle> dh(store.retrieve(field));
    
        std::vector<char> test(dh->size());
        dh->openForRead();
        {
            eckit::AutoClose closer(*dh);
            dh->read(&test[0], test.size() - 3);
        }
        eckit::MemoryHandle mh;
        dh->copyTo(mh);
        EXPECT(mh.size() == eckit::Length(sizeof(data)));
        EXPECT(::memcmp(mh.data(), data, sizeof(data)) == 0);

        // remove data

        /// @todo: should DaosStore::remove accept full URIs to field arrays and remove the store container?
        eckit::PathName store_path{field.location().uri().path()};
        std::ostream out(std::cout.rdbuf());
        store.remove(field.location().uri(), out, out, false);
        EXPECT(store_path.exists());
        store.remove(field.location().uri(), out, out, true);
        EXPECT_NOT(store_path.exists());

        // deindex data

        {
            fdb5::DaosCatalogueWriter dcat{db_key, config};
            fdb5::Catalogue& cat = static_cast<fdb5::Catalogue&>(dcat);
            std::ostream out(std::cout.rdbuf());
            metkit::mars::MarsRequest r = db_key.request("retrieve");
            std::unique_ptr<fdb5::WipeVisitor> wv(cat.wipeVisitor(store, r, out, true, false, false));
            cat.visitEntries(*wv, store, false);
        }

        /// @todo: again, daos_fini happening before

    }

    SECTION("Via FDB API with a DAOS catalogue and store") {

        // FDB configuration

        int container_oids_per_alloc_small = 100;

        std::string config_str{
            "type: local\n"
            "schema : " + schema_file().path() + "\n"
            "engine: daos\n"
            "store: daos\n"
            "daos:\n"
            "  catalogue:\n"
            "    pool: " + pool_name + "\n"
            "    root_cont: " + root_cont_name + "\n"
            "  store:\n"
            "    pool: " + pool_name + "\n"
            "  client:\n"
            "    container_oids_per_alloc: " + std::to_string(container_oids_per_alloc_small)
        };

        fdb5::Config config{YAMLConfiguration(config_str)};

        // request

        fdb5::Key request_key{"a=11,b=22,c=3,d=4,e=5,f=6"};
        fdb5::Key index_key{"a=11,b=22,c=3,d=4"};
        fdb5::Key db_key{"a=11,b=22"};

        fdb5::FDBToolRequest full_req{
            request_key.request("retrieve"), 
            false, 
            std::vector<std::string>{"a", "b"}
        };
        fdb5::FDBToolRequest index_req{
            index_key.request("retrieve"), 
            false, 
            std::vector<std::string>{"a", "b"}
        };
        fdb5::FDBToolRequest db_req{
            db_key.request("retrieve"), 
            false, 
            std::vector<std::string>{"a", "b"}
        };
        fdb5::FDBToolRequest all_req{
            metkit::mars::MarsRequest{}, 
            true, 
            std::vector<std::string>{}
        };

        // initialise store

        fdb5::FDB fdb(config);

        // check store is empty

        size_t count;
        fdb5::ListElement info;

        /// @todo: here, DaosManager is being configured with DAOS client config passed to FDB instance constructor.
        //   It happens in EntryVisitMechanism::visit when calling DB::open. Is this OK, or should this configuring
        //   rather happen as part of transforming a FieldLocation into a DataHandle? It is probably OK. One thing
        //   is to configure the DAOS client and the other thing is to initialise it.
        auto listObject = fdb.list(db_req);

        count = 0;
        while (listObject.next(info)) {
            info.print(std::cout, true, true);
            std::cout << std::endl;
            ++count;
        }
        EXPECT(count == 0);

        // store data

        char data[] = "test";

        /// @todo: here, DaosManager is being reconfigured with identical config, and it happens again multiple times below.
        //   Should this be avoided?
        fdb.archive(request_key, data, sizeof(data));

        fdb.flush();

        // retrieve data

        metkit::mars::MarsRequest r = request_key.request("retrieve");
        std::unique_ptr<eckit::DataHandle> dh(fdb.retrieve(r));
    
        eckit::MemoryHandle mh;
        dh->copyTo(mh);
        EXPECT(mh.size() == eckit::Length(sizeof(data)));
        EXPECT(::memcmp(mh.data(), data, sizeof(data)) == 0);

        // test io statistics

        fdb5::DaosManager::instance().stats().report(std::cout);

        // list all

        listObject = fdb.list(all_req);
        count = 0;
        while (listObject.next(info)) {
            // info.print(std::cout, true, true);
            // std::cout << std::endl;
            count++;
        }
        EXPECT(count == 1);

        // wipe data

        fdb5::WipeElement elem;

        // dry run attempt to wipe with too specific request

        auto wipeObject = fdb.wipe(full_req);
        count = 0;
        while (wipeObject.next(elem)) count++;
        EXPECT(count == 0);

        // dry run wipe index and store unit
        wipeObject = fdb.wipe(index_req);
        count = 0;
        while (wipeObject.next(elem)) count++;
        EXPECT(count > 0);

        // dry run wipe database
        wipeObject = fdb.wipe(db_req);
        count = 0;
        while (wipeObject.next(elem)) count++;
        EXPECT(count > 0);

        // ensure field still exists
        listObject = fdb.list(full_req);
        count = 0;
        while (listObject.next(info)) {
            // info.print(std::cout, true, true);
            // std::cout << std::endl;
            count++;
        }
        EXPECT(count == 1);

        // attempt to wipe with too specific request
        wipeObject = fdb.wipe(full_req, true);
        count = 0;
        while (wipeObject.next(elem)) count++;
        EXPECT(count == 0);
        /// @todo: really needed?
        fdb.flush();

        // wipe index and store unit
        wipeObject = fdb.wipe(index_req, true);
        count = 0;
        while (wipeObject.next(elem)) count++;
        EXPECT(count > 0);
        /// @todo: really needed?
        fdb.flush();

        // ensure field does not exist
        listObject = fdb.list(full_req);
        count = 0;
        while (listObject.next(info)) count++;
        EXPECT(count == 0);

        // /// @todo: ensure index and corresponding container do not exist
        // /// @todo: ensure DB still exists
        // /// @todo: list db or index and expect count = 0?

        // /// @todo: ensure new DaosSession has updated daos client config

    }

    /// @todo: if doing what's in this section at the end of the previous section reusing the same FDB object,
    // archive() fails as it expects a toc file to exist, but it has been removed by previous wipe
    SECTION("FDB API RE-STORE AND WIPE DB") {

        // FDB configuration

        std::string config_str{
            "type: local\n"
            "schema : " + schema_file().path() + "\n"
            "engine: toc\n"
            "store: daos\n"
            "daos:\n"
            "  store:\n"
            "    pool: " + pool_name + "\n"
            "  client:\n"
            "    container_oids_per_alloc: " + std::to_string(container_oids_per_alloc)
        };

        fdb5::Config config{YAMLConfiguration(config_str)};

        // request

        fdb5::Key request_key{"a=11,b=22,c=3,d=4,e=5,f=6"};
        fdb5::Key index_key{"a=11,b=22,c=3,d=4"};
        fdb5::Key db_key{"a=11,b=22"};

        fdb5::FDBToolRequest full_req{
            request_key.request("retrieve"), 
            false, 
            std::vector<std::string>{"a", "b"}
        };
        fdb5::FDBToolRequest index_req{
            index_key.request("retrieve"), 
            false, 
            std::vector<std::string>{"a", "b"}
        };
        fdb5::FDBToolRequest db_req{
            db_key.request("retrieve"), 
            false, 
            std::vector<std::string>{"a", "b"}
        };

        // initialise store

        fdb5::FDB fdb(config);

        // store again

        char data[] = "test";

        fdb.archive(request_key, data, sizeof(data));

        fdb.flush();

        size_t count;
        
        // wipe all database

        fdb5::WipeElement elem;
        auto wipeObject = fdb.wipe(db_req, true);
        count = 0;
        while (wipeObject.next(elem)) count++;
        EXPECT(count > 0);
        /// @todo: really needed?
        fdb.flush();

        // ensure field does not exist

        fdb5::ListElement info;
        auto listObject = fdb.list(full_req);
        count = 0;
        while (listObject.next(info)) {
            // info.print(std::cout, true, true);
            // std::cout << std::endl;
            count++;
        }
        EXPECT(count == 0);

        /// @todo: ensure DB and corresponding pool do not exist

    }

    // teardown daos

#ifdef fdb5_HAVE_DAOS_ADMIN
    /// AutoPoolDestroy is not possible here because the pool is 
    /// created above with an ephemeral session
    fdb5::DaosSession().destroyPool(pool_uuid);
#else
    for (auto& c : fdb5::DaosSession().getPool(pool_uuid).listContainers())
        if (c == root_cont_name || c == "11:22")
            fdb5::DaosSession().getPool(pool_uuid).destroyContainer(c);
#endif

}

}  // namespace test
}  // namespace fdb

int main(int argc, char **argv)
{
    return run_tests ( argc, argv );
}
