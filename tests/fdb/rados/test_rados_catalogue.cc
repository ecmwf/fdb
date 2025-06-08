/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

// #include <cstring>
// #include <memory>

#include "eckit/config/Resource.h"
#include "eckit/testing/Test.h"
// #include "eckit/filesystem/URI.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/TmpFile.h"
// #include "eckit/filesystem/TmpDir.h"
// #include "eckit/io/FileHandle.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/config/YAMLConfiguration.h"
#include "eckit/io/rados/RadosPartHandle.h"

// #include "metkit/mars/MarsRequest.h"

#include "fdb5/fdb5_config.h"
// #include "fdb5/config/Config.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

#include "fdb5/toc/TocStore.h"

// #include "fdb5/daos/DaosSession.h"
// #include "fdb5/daos/DaosPool.h"
// #include "fdb5/daos/DaosArrayPartHandle.h"

#include "fdb5/rados/RadosStore.h"
#include "fdb5/rados/RadosFieldLocation.h"
#include "fdb5/rados/RadosCatalogueWriter.h"
#include "fdb5/rados/RadosCatalogueReader.h"

using namespace eckit::testing;
using namespace eckit;

namespace {

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
    };

    void ensureCleanNamespaces(const std::string& pool, const std::string& prefix) {
        ASSERT(prefix.length() > 3);
        for (const std::string& name : eckit::RadosCluster::instance().listNamespaces(pool)) {
            if (name.rfind(prefix, 0) == 0) {
                eckit::RadosNamespace{pool, name}.destroy();
            }
        }
    }

#ifdef fdb5_HAVE_RADOS_ADMIN
    void ensureClean(const std::string& prefix) {
        ASSERT(prefix.length() > 3);
        for (const std::string& name : eckit::RadosCluster::instance().listPools()) {
            if (name.rfind(prefix, 0) == 0) {
                eckit::RadosPool{name}.destroy();
            }
        }
    }
#endif

}

// temporary schema,spaces,root files common to all DAOS Catalogue tests

eckit::TmpFile& schema_file() {
    static eckit::TmpFile f{};
    return f;
}

eckit::TmpFile& opt_schema_file() {
    static eckit::TmpFile f{};
    return f;
}

eckit::PathName& catalogue_tests_tmp_root() {
    static eckit::PathName cd("./rados_catalogue_tests_fdb_root");
    return cd;
}

namespace fdb {
namespace test {

CASE( "Setup" ) {

#if !defined(fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL) && !defined(fdb5_HAVE_RADOS_ADMIN)
    throw eckit::Exception(
        "RadosStore unit tests require Rados admin permissions to create pools if "
        "RADOS_BACKENDS_SINGLE_POOL=OFF, and require enabling RADOS_ADMIN=ON.");
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

    std::string opt_schema_str{"[ a, b [ c?, d [ e?, f ]]]"};

    std::unique_ptr<eckit::DataHandle> hs_opt(opt_schema_file().fileHandle());
    hs_opt->openForWrite(opt_schema_str.size());
    {
        eckit::AutoClose closer(*hs_opt);
        hs_opt->write(opt_schema_str.data(), opt_schema_str.size());
    }

    // this is necessary to avoid ~fdb/etc/fdb/schema being used where
    // LibFdb5::instance().defaultConfig().schema() is called
    // due to no specified schema file (e.g. in Key::registry())
    ::setenv("FDB_SCHEMA_FILE", schema_file().path().c_str(), 1);

}

CASE("RadosCatalogue tests") {

    std::string test_id = "test-catalogue";
#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL
  #ifdef eckit_HAVE_RADOS_ADMIN
    std::string pool = test_id;
    eckit::RadosPool{pool}.ensureDestroyed();
    eckit::RadosPool{pool}.ensureCreated();  /// @todo: auto pool destroyer
  #else
    std::string pool;
    pool = eckit::Resource<std::string>(
        "fdbRadosTestPool;$FDB_RADOS_TEST_POOL", pool
    );
    EXPECT(pool.length() > 0);
    ensureCleanNamespaces(pool, test_id);
  #endif
#else
    std::string prefix = test_id;
    ensureClean(prefix);
#endif

    SECTION("DaosCatalogue archive (index) and retrieve without a Store") {

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL
        std::string config_str{
            "spaces:\n"
            "- roots:\n"
            "  - path: " + catalogue_tests_tmp_root().asString() + "\n"
            "schema : " + schema_file().path() + "\n"
            "rados:\n"
            "  catalogue:\n"
            "    pool: " + pool + "\n"
            "    root_namespace: " + test_id + "_root\n"
            "    namespace_prefix: " + test_id + "\n" 
        };
#else
        std::string config_str{
            "spaces:\n"
            "- roots:\n"
            "  - path: " + catalogue_tests_tmp_root().asString() + "\n"
            "schema : " + schema_file().path() + "\n"
            "rados:\n"
            "  catalogue:\n"
            "    namespace: default\n"
            "    root_pool: " + prefix + "_root\n"
            "    pool_prefix: " + prefix + "\n"
        };
#endif

        fdb5::Config config{YAMLConfiguration(config_str)};
        fdb5::Schema schema{schema_file()};

        /// @note: a=11,b=22 instead of a=1,b=2 to avoid collision with potential parallel runs of store tests using a=1,b=2
        fdb5::Key request_key({{"a", "11"}, {"b", "22"}, {"c", "3"}, {"d", "4"}, {"e", "5"}, {"f", "6"}});
        fdb5::Key db_key({{"a", "11"}, {"b", "22"}}, schema.registry());
        fdb5::Key index_key({{"c", "3"}, {"d", "4"}}, schema.registry());
        fdb5::Key field_key({{"e", "5"}, {"f", "6"}}, schema.registry());

        // archive

        std::unique_ptr<fdb5::FieldLocation> loc(new fdb5::RadosFieldLocation(
            eckit::URI{"rados", "test_uri"}, eckit::Offset(0), eckit::Length(1), fdb5::Key(nullptr, true)
        ));

        {
            fdb5::RadosCatalogueWriter dcatw{db_key, config};

        //     fdb5::DaosName db_cont{pool_name, db_key.valuesToString()};
        //     fdb5::DaosKeyValueOID cat_kv_oid{0, 0, OC_S1};  /// @todo: take oclass from config
        //     fdb5::DaosKeyValueName cat_kv{pool_name, db_key.valuesToString(), cat_kv_oid};
        //     EXPECT(db_cont.exists());
        //     EXPECT(cat_kv.exists());

            fdb5::Catalogue& cat = dcatw;
            cat.selectIndex(index_key);
        //     fdb5::DaosKeyValueOID index_kv_oid{index_key.valuesToString(), OC_S1};  /// @todo: take oclass from config
        //     fdb5::DaosKeyValueName index_kv{pool_name, db_key.valuesToString(), index_kv_oid};
        //     EXPECT(index_kv.exists());
        //     EXPECT(cat_kv.has(index_key.valuesToString()));

            fdb5::CatalogueWriter& catw = dcatw;
            catw.archive(field_key, std::move(loc));
            cat.flush();
        //     EXPECT(index_kv.has(field_key.valuesToString()));
        //     fdb5::DaosKeyValueOID e_axis_kv_oid{index_key.valuesToString() + std::string{".e"}, OC_S1};
        //     fdb5::DaosKeyValueName e_axis_kv{pool_name, db_key.valuesToString(), e_axis_kv_oid};
        //     EXPECT(e_axis_kv.exists());
        //     EXPECT(e_axis_kv.has("5"));
        //     fdb5::DaosKeyValueOID f_axis_kv_oid{index_key.valuesToString() + std::string{".f"}, OC_S1};
        //     fdb5::DaosKeyValueName f_axis_kv{pool_name, db_key.valuesToString(), f_axis_kv_oid};
        //     EXPECT(f_axis_kv.exists());
        //     EXPECT(f_axis_kv.has("6"));
        }

        // retrieve

        {
            fdb5::RadosCatalogueReader dcatr{db_key, config};

            fdb5::Catalogue& cat = dcatr;
            cat.selectIndex(index_key);

            fdb5::Field f;
            fdb5::CatalogueReader& catr = dcatr;
            catr.retrieve(field_key, f);
            EXPECT(f.location().uri().name() == eckit::URI("rados", "test_uri").name());
            EXPECT(f.location().offset() == eckit::Offset(0));
            EXPECT(f.location().length() == eckit::Length(1));
        }

        // // remove (manual deindex)

        // {
        //     fdb5::DaosCatalogueWriter dcatw{db_key, config};
        //     fdb5::DaosName db_cont{dcatw.uri()};
        //     std::ostream out(std::cout.rdbuf());

        //     fdb5::DaosCatalogue::remove(db_cont, out, out, true);

        //     fdb5::DaosKeyValueOID cat_kv_oid{0, 0, OC_S1};  /// @todo: take oclass from config
        //     fdb5::DaosKeyValueName cat_kv{pool_name, db_key.valuesToString(), cat_kv_oid};
        //     EXPECT_NOT(cat_kv.exists());
        //     EXPECT_NOT(db_cont.exists());
        // }

    }

    SECTION("RadosCatalogue archive (index) and retrieve with a RadosStore") {

        // FDB configuration

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL
        std::string config_str{
            "spaces:\n"
            "- roots:\n"
            "  - path: " + catalogue_tests_tmp_root().asString() + "\n"
            "schema : " + schema_file().path() + "\n"
            "rados:\n"
            "  pool: " + pool + "\n"
            "  root_namespace: " + test_id + "_root\n"
            "  namespace_prefix: " + test_id + "\n" 
        };
#else
        std::string config_str{
            "spaces:\n"
            "- roots:\n"
            "  - path: " + catalogue_tests_tmp_root().asString() + "\n"
            "schema : " + schema_file().path() + "\n"
            "rados:\n"
            "  namespace: default\n"
            "  root_pool: " + prefix + "_root\n"
            "  pool_prefix: " + prefix + "\n"
        };
#endif

        fdb5::Config config{YAMLConfiguration(config_str)};

        // schema

        fdb5::Schema schema{schema_file()};

        // request

        fdb5::Key request_key({{"a", "11"}, {"b", "22"}, {"c", "3"}, {"d", "4"}, {"e", "5"}, {"f", "6"}});
        fdb5::Key db_key({{"a", "11"}, {"b", "22"}});
        fdb5::Key index_key({{"c", "3"}, {"d", "4"}});
        fdb5::Key field_key({{"e", "5"}, {"f", "6"}});

        // store data

        char data[] = "test";

        fdb5::RadosStore rstore{schema, db_key, config};
        fdb5::Store& store = static_cast<fdb5::Store&>(rstore);
        std::unique_ptr<fdb5::FieldLocation> loc(store.archive(index_key, data, sizeof(data)));

        // index data

        {
            fdb5::RadosCatalogueWriter rcatw{db_key, config};
            fdb5::Catalogue& cat = rcatw;
            cat.deselectIndex();
            cat.selectIndex(index_key);
            fdb5::CatalogueWriter& catw = rcatw;
            catw.archive(field_key, std::move(loc));

            /// flush store before flushing catalogue
            rstore.flush();  // not necessary if using a DAOS store
        }

        // find data

        fdb5::Field field;
        {
            fdb5::RadosCatalogueReader rcatr{db_key, config};
            fdb5::Catalogue& cat = rcatr;
            cat.selectIndex(index_key);
            fdb5::CatalogueReader& catr = rcatr;
            catr.retrieve(field_key, field);
        }
        std::cout << "Read location: " << field.location() << std::endl;

        // retrieve data

        std::unique_ptr<eckit::DataHandle> dh(store.retrieve(field));
        EXPECT(dynamic_cast<eckit::RadosPartHandle*>(dh.get()));
    
        eckit::MemoryHandle mh;
        dh->copyTo(mh);
        EXPECT(mh.size() == eckit::Length(sizeof(data)));
        EXPECT(::memcmp(mh.data(), data, sizeof(data)) == 0);

    //     // deindex data

    //     {
    //         fdb5::DaosCatalogueWriter dcat{db_key, config};
    //         fdb5::Catalogue& cat = static_cast<fdb5::Catalogue&>(dcat);
    //         std::ostream out(std::cout.rdbuf());
    //         metkit::mars::MarsRequest r = db_key.request("retrieve");
    //         std::unique_ptr<fdb5::WipeVisitor> wv(cat.wipeVisitor(store, r, out, true, false, false));
    //         cat.visitEntries(*wv, store, false);
    //     }

    }

    // SECTION("DaosCatalogue archive (index) and retrieve with a TocStore") {

    //     // FDB configuration

    //     std::string config_str{
    //         "spaces:\n"
    //         "- roots:\n"
    //         "  - path: " + catalogue_tests_tmp_root().asString() + "\n"
    //         "schema : " + schema_file().path() + "\n"
    //         "daos:\n"
    //         "  catalogue:\n"
    //         "    pool: " + pool_name + "\n"
    //         "    root_cont: " + root_cont_name + "\n"
    //         "  client:\n"
    //         "    container_oids_per_alloc: " + std::to_string(container_oids_per_alloc)
    //     };

    //     fdb5::Config config{YAMLConfiguration(config_str)};

    //     // schema

    //     fdb5::Schema schema{schema_file()};

    //     // request

    //     fdb5::Key request_key({{"a", "11"}, {"b", "22"}, {"c", "3"}, {"d", "4"}, {"e", "5"}, {"f", "6"}});
    //     fdb5::Key db_key({{"a", "11"}, {"b", "22"}});
    //     fdb5::Key index_key({{"c", "3"}, {"d", "4"}});
    //     fdb5::Key field_key({{"e", "5"}, {"f", "6"}});

    //     // store data

    //     char data[] = "test";

    //     fdb5::TocStore tstore{schema, db_key, config};
    //     fdb5::Store& store = static_cast<fdb5::Store&>(tstore);
    //     std::unique_ptr<fdb5::FieldLocation> loc(store.archive(index_key, data, sizeof(data)));
    //     /// @todo: there are two cont create with label here
    //     /// @todo: again, daos_fini happening before cont and pool close

    //     // index data

    //     {
    //         fdb5::DaosCatalogueWriter dcatw{db_key, config};
    //         fdb5::Catalogue& cat = dcatw;
    //         cat.deselectIndex();
    //         cat.selectIndex(index_key);
    //         fdb5::CatalogueWriter& catw = dcatw;
    //         catw.archive(field_key, std::move(loc));

    //         /// flush store before flushing catalogue
    //         tstore.flush();
    //     }

    //     // find data

    //     fdb5::Field field;
    //     {
    //         fdb5::DaosCatalogueReader dcatr{db_key, config};
    //         fdb5::Catalogue& cat = dcatr;
    //         cat.selectIndex(index_key);
    //         fdb5::CatalogueReader& catr = dcatr;
    //         catr.retrieve(field_key, field);
    //     }
    //     std::cout << "Read location: " << field.location() << std::endl;

    //     // retrieve data

    //     std::unique_ptr<eckit::DataHandle> dh(store.retrieve(field));
    
    //     std::vector<char> test(dh->size());
    //     dh->openForRead();
    //     {
    //         eckit::AutoClose closer(*dh);
    //         dh->read(&test[0], test.size() - 3);
    //     }
    //     eckit::MemoryHandle mh;
    //     dh->copyTo(mh);
    //     EXPECT(mh.size() == eckit::Length(sizeof(data)));
    //     EXPECT(::memcmp(mh.data(), data, sizeof(data)) == 0);

    //     // remove data

    //     /// @todo: should DaosStore::remove accept full URIs to field arrays and remove the store container?
    //     eckit::PathName store_path{field.location().uri().path()};
    //     std::ostream out(std::cout.rdbuf());
    //     store.remove(field.location().uri(), out, out, false);
    //     EXPECT(store_path.exists());
    //     store.remove(field.location().uri(), out, out, true);
    //     EXPECT_NOT(store_path.exists());

    //     // deindex data

    //     {
    //         fdb5::DaosCatalogueWriter dcat{db_key, config};
    //         fdb5::Catalogue& cat = static_cast<fdb5::Catalogue&>(dcat);
    //         std::ostream out(std::cout.rdbuf());
    //         metkit::mars::MarsRequest r = db_key.request("retrieve");
    //         std::unique_ptr<fdb5::WipeVisitor> wv(cat.wipeVisitor(store, r, out, true, false, false));
    //         cat.visitEntries(*wv, store, false);
    //     }

    //     /// @todo: again, daos_fini happening before

    // }

    SECTION("Via FDB API with a Rados catalogue and store") {

        // FDB configuration

        std::string config_str{
            "spaces:\n"
            "- roots:\n"
            "  - path: " + catalogue_tests_tmp_root().asString() + "\n"
            "type: local\n"
            "schema : " + schema_file().path() + "\n"
            "engine: rados\n"
            "store: rados\n"
            "rados:\n"
        };

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL
        config_str += "  pool: " + pool + "\n"
            "  root_namespace: " + test_id + "_root\n"
            "  namespace_prefix: " + test_id + "\n";
#else
        config_str += "  namespace: default\n"
            "  root_pool: " + prefix + "_root\n"
            "  pool_prefix: " + prefix + "\n";
#endif

        fdb5::Config config{YAMLConfiguration(config_str)};

        // request

        fdb5::Key request_key({{"a", "11"}, {"b", "22"}, {"c", "3"}, {"d", "4"}, {"e", "5"}, {"f", "6"}});
        fdb5::Key db_key({{"a", "11"}, {"b", "22"}});
        fdb5::Key index_key({{"a", "11"}, {"b", "22"}, {"c", "3"}, {"d", "4"}});

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

        // initialise FDB

        fdb5::FDB fdb(config);

        // check FDB is empty

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

        // archive data

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

        // list all

        listObject = fdb.list(all_req);
        count = 0;
        while (listObject.next(info)) {
            // info.print(std::cout, true, true);
            // std::cout << std::endl;
            count++;
        }
        EXPECT(count == 1);

        // // wipe data

        // fdb5::WipeElement elem;

        // // dry run attempt to wipe with too specific request

        // auto wipeObject = fdb.wipe(full_req);
        // count = 0;
        // while (wipeObject.next(elem)) count++;
        // EXPECT(count == 0);

        // // dry run wipe index and store unit
        // wipeObject = fdb.wipe(index_req);
        // count = 0;
        // while (wipeObject.next(elem)) count++;
        // EXPECT(count > 0);

        // // dry run wipe database
        // wipeObject = fdb.wipe(db_req);
        // count = 0;
        // while (wipeObject.next(elem)) count++;
        // EXPECT(count > 0);

        // // ensure field still exists
        // listObject = fdb.list(full_req);
        // count = 0;
        // while (listObject.next(info)) {
        //     // info.print(std::cout, true, true);
        //     // std::cout << std::endl;
        //     count++;
        // }
        // EXPECT(count == 1);

        // // attempt to wipe with too specific request
        // wipeObject = fdb.wipe(full_req, true);
        // count = 0;
        // while (wipeObject.next(elem)) count++;
        // EXPECT(count == 0);
        // /// @todo: really needed?
        // fdb.flush();

        // // wipe index and store unit
        // wipeObject = fdb.wipe(index_req, true);
        // count = 0;
        // while (wipeObject.next(elem)) count++;
        // EXPECT(count > 0);
        // /// @todo: really needed?
        // fdb.flush();

        // // ensure field does not exist
        // listObject = fdb.list(full_req);
        // count = 0;
        // while (listObject.next(info)) count++;
        // EXPECT(count == 0);

        // /// @todo: ensure index and corresponding container do not exist
        // /// @todo: ensure DB still exists
        // /// @todo: list db or index and expect count = 0?

        // // re-archive data

        // /// @note: FDB holds a LocalFDB which holds an Archiver which holds open DBs (DaosCatalogueWriters).
        // ///   If a whole DB is wiped, the top-level structures for that DB (main and catalogue KVs in this case)
        // ///   are deleted. If willing to archive again into that DB, the DB needs to be constructed again as the 
        // ///   top-level structures are only generated as part of the DaosCatalogueWriter constructor. There is
        // ///   no way currently to destroy the open DBs held by FDB other than entirely destroying FDB.
        // ///   Alternatively, a separate FDB instance can be created.
        // fdb5::FDB fdb2(config);

        // fdb2.archive(request_key, data, sizeof(data));

        // fdb2.flush();

        // listObject = fdb2.list(full_req);
        // count = 0;
        // while (listObject.next(info)) {
        //     // info.print(std::cout, true, true);
        //     // std::cout << std::endl;
        //     count++;
        // }
        // EXPECT(count == 1);
        
        // // wipe full database

        // wipeObject = fdb2.wipe(db_req, true);
        // count = 0;
        // while (wipeObject.next(elem)) count++;
        // EXPECT(count > 0);
        // /// @todo: really needed?
        // fdb2.flush();

        // // ensure field does not exist

        // listObject = fdb2.list(full_req);
        // count = 0;
        // while (listObject.next(info)) {
        //     // info.print(std::cout, true, true);
        //     // std::cout << std::endl;
        //     count++;
        // }
        // EXPECT(count == 0);

        // /// @todo: ensure DB and corresponding pool do not exist

        // /// @todo: ensure new DaosSession has updated daos client config

    }

    // SECTION("OPTIONAL SCHEMA KEYS") {

    //     // FDB configuration

    //     ::setenv("FDB_SCHEMA_FILE", opt_schema_file().path().c_str(), 1);

    //     std::string config_str{
    //         "spaces:\n"
    //         "- roots:\n"
    //         "  - path: " + catalogue_tests_tmp_root().asString() + "\n"
    //         "type: local\n"
    //         "schema : " + opt_schema_file().path() + "\n"
    //         "engine: daos\n"
    //         "store: daos\n"
    //         "daos:\n"
    //         "  catalogue:\n"
    //         "    pool: " + pool_name + "\n"
    //         "    root_cont: " + root_cont_name + "\n"
    //         "  store:\n"
    //         "    pool: " + pool_name + "\n"
    //         "  client:\n"
    //         "    container_oids_per_alloc: " + std::to_string(container_oids_per_alloc)
    //     };

    //     fdb5::Config config{YAMLConfiguration(config_str)};

    //     // request

    //     fdb5::Key request_key({{"a", "11"}, {"b", "22"}, {"d", "4"}, {"f", "6"}});
    //     fdb5::Key request_key2({{"a", "11"}, {"b", "22"}, {"d", "4"}, {"e", "5"}, {"f", "6"}});
    //     fdb5::Key db_key({{"a", "11"}, {"b", "22"}});
    //     fdb5::Key index_key({{"a", "11"}, {"b", "22"}, {"d", "4"}});

    //     fdb5::FDBToolRequest full_req{
    //         request_key.request("retrieve"), 
    //         false, 
    //         std::vector<std::string>{"a", "b"}
    //     };
    //     fdb5::FDBToolRequest full_req2{
    //         request_key2.request("retrieve"), 
    //         false, 
    //         std::vector<std::string>{"a", "b"}
    //     };
    //     fdb5::FDBToolRequest index_req{
    //         index_key.request("retrieve"), 
    //         false, 
    //         std::vector<std::string>{"a", "b"}
    //     };
    //     fdb5::FDBToolRequest db_req{
    //         db_key.request("retrieve"), 
    //         false, 
    //         std::vector<std::string>{"a", "b"}
    //     };
    //     fdb5::FDBToolRequest all_req{
    //         metkit::mars::MarsRequest{}, 
    //         true, 
    //         std::vector<std::string>{}
    //     };

    //     // initialise FDB

    //     fdb5::FDB fdb(config);

    //     // check FDB is empty

    //     size_t count;
    //     fdb5::ListElement info;

    //     auto listObject = fdb.list(db_req);

    //     count = 0;
    //     while (listObject.next(info)) {
    //         info.print(std::cout, true, true);
    //         std::cout << std::endl;
    //         ++count;
    //     }
    //     EXPECT(count == 0);

    //     // archive data with incomplete key

    //     char data[] = "test";

    //     fdb.archive(request_key, data, sizeof(data));

    //     fdb.flush();

    //     // list data

    //     listObject = fdb.list(db_req);

    //     count = 0;
    //     while (listObject.next(info)) {
    //         info.print(std::cout, true, true);
    //         std::cout << std::endl;
    //         ++count;
    //     }
    //     EXPECT(count == 1);

    //     // retrieve data

    //     {
    //         metkit::mars::MarsRequest r = request_key.request("retrieve");
    //         std::unique_ptr<eckit::DataHandle> dh(fdb.retrieve(r));

    //         eckit::MemoryHandle mh;
    //         dh->copyTo(mh);
    //         EXPECT(mh.size() == eckit::Length(sizeof(data)));
    //         EXPECT(::memcmp(mh.data(), data, sizeof(data)) == 0);
    //     }

    //     // archive data with complete key

    //     char data2[] = "abcd";

    //     fdb.archive(request_key2, data2, sizeof(data));

    //     fdb.flush();

    //     // list data

    //     listObject = fdb.list(db_req);

    //     count = 0;
    //     while (listObject.next(info)) {
    //         info.print(std::cout, true, true);
    //         std::cout << std::endl;
    //         ++count;
    //     }
    //     EXPECT(count == 2);

    //     // retrieve data

    //     {
    //         metkit::mars::MarsRequest r = request_key.request("retrieve");
    //         std::unique_ptr<eckit::DataHandle> dh(fdb.retrieve(r));

    //         eckit::MemoryHandle mh;
    //         dh->copyTo(mh);
    //         EXPECT(mh.size() == eckit::Length(sizeof(data)));
    //         EXPECT(::memcmp(mh.data(), data, sizeof(data)) == 0);
    //     }

    //     {
    //         metkit::mars::MarsRequest r = request_key2.request("retrieve");
    //         std::unique_ptr<eckit::DataHandle> dh(fdb.retrieve(r));

    //         eckit::MemoryHandle mh;
    //         dh->copyTo(mh);
    //         EXPECT(mh.size() == eckit::Length(sizeof(data2)));
    //         EXPECT(::memcmp(mh.data(), data2, sizeof(data2)) == 0);
    //     }

    // }

    // teardown rados

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL
  #ifdef eckit_HAVE_RADOS_ADMIN
    eckit::RadosPool{pool}.ensureDestroyed();
  #else
    ensureCleanNamespaces(pool, test_id);
  #endif
#else
    ensureClean(prefix);
#endif

}

}  // namespace test
}  // namespace fdb

int main(int argc, char **argv)
{
    return run_tests ( argc, argv );
}
