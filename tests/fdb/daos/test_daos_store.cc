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
#include "eckit/testing/Filesystem.h"
#include "eckit/testing/Test.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/api/local/WipeVisitor.h"
#include "fdb5/config/Config.h"
#include "fdb5/fdb5_config.h"

#include "fdb5/toc/TocCatalogueReader.h"
#include "fdb5/toc/TocCatalogueWriter.h"

#include "fdb5/daos/DaosArrayPartHandle.h"
#include "fdb5/daos/DaosPool.h"
#include "fdb5/daos/DaosSession.h"

#include "fdb5/daos/DaosException.h"
#include "fdb5/daos/DaosFieldLocation.h"
#include "fdb5/daos/DaosStore.h"

using namespace eckit::testing;
using namespace eckit;

#ifdef fdb5_HAVE_DUMMY_DAOS
eckit::PathName& tmp_dummy_daos_root() {
    static eckit::PathName d("./daos_store_tests_dummy_daos_root");
    return d;
}
#endif

// temporary schema,spaces,root files common to all DAOS Store tests

eckit::TmpFile& schema_file() {
    static eckit::TmpFile f{};
    return f;
}

eckit::PathName& store_tests_tmp_root() {
    static eckit::PathName sd("./daos_store_tests_fdb_root");
    return sd;
}

size_t countWipeable(fdb5::WipeIterator& wipeObject, bool print = true) {
    size_t count = 0;
    fdb5::WipeElement elem;
    while (wipeObject.next(elem)) {
        if (print)
            std::cout << elem << std::endl;
        if (elem.type() != fdb5::WipeElementType::ERROR && elem.type() != fdb5::WipeElementType::CATALOGUE_INFO &&
            elem.type() != fdb5::WipeElementType::CATALOGUE_SAFE && elem.type() != fdb5::WipeElementType::STORE_SAFE) {
            count += elem.uris().size();
        }
    }
    return count;
}

namespace fdb {
namespace test {

CASE("DaosStore tests") {

    // setup

#ifdef fdb5_HAVE_DUMMY_DAOS
    if (tmp_dummy_daos_root().exists())
        testing::deldir(tmp_dummy_daos_root());
    tmp_dummy_daos_root().mkdir();
    ::setenv("DUMMY_DAOS_DATA_ROOT", tmp_dummy_daos_root().path().c_str(), 1);
#endif

    // ensure fdb root directory exists. If not, then that root is
    // registered as non existing and Store tests fail.
    if (store_tests_tmp_root().exists())
        testing::deldir(store_tests_tmp_root());
    store_tests_tmp_root().mkdir();
    ::setenv("FDB_ROOT_DIRECTORY", store_tests_tmp_root().path().c_str(), 1);

    // prepare schema for tests involving DaosStore

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

    // test parameters

    int container_oids_per_alloc = 1000;
#if defined(fdb5_HAVE_DAOS_ADMIN) || defined(fdb5_HAVE_DUMMY_DAOS)
    std::string pool_name{"fdb_pool"};
#else
    std::string pool_name;
    pool_name = eckit::Resource<std::string>("fdbDaosTestPool;$FDB_DAOS_TEST_POOL", pool_name);
    EXPECT(pool_name.length() > 0);
#endif

    // bootstrap daos

    fdb5::UUID pool_uuid;
    {
        fdb5::DaosManager::instance().configure(eckit::LocalConfiguration(
            YAMLConfiguration("container_oids_per_alloc: " + std::to_string(container_oids_per_alloc))));
        fdb5::DaosSession s{};
#ifdef fdb5_HAVE_DAOS_ADMIN
        fdb5::DaosPool& pool = s.createPool(pool_name);
#else
#ifdef fdb5_HAVE_DUMMY_DAOS
        std::string pool_uuid_str{"00000000-0000-0000-0000-000000000003"};
        (tmp_dummy_daos_root() / pool_uuid_str).mkdir();
        ::symlink((tmp_dummy_daos_root() / pool_uuid_str).path().c_str(),
                  (tmp_dummy_daos_root() / pool_name).path().c_str());
#endif
        fdb5::DaosPool& pool = s.getPool(pool_name);
#endif
        pool_uuid = pool.uuid();
    }

    SECTION("archive and retrieve") {

        std::string config_str{
            "spaces:\n"
            "- roots:\n"
            "  - path: " +
            store_tests_tmp_root().asString() +
            "\n"
            "daos:\n"
            "  store:\n"
            "    pool: " +
            pool_name +
            "\n"
            "  client:\n"
            "    container_oids_per_alloc: " +
            std::to_string(container_oids_per_alloc)};

        fdb5::Config config{YAMLConfiguration(config_str)};

        fdb5::Schema schema{schema_file()};

        fdb5::Key request_key({{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"}, {"e", "5"}, {"f", "6"}});
        fdb5::Key db_key({{"a", "1"}, {"b", "2"}});
        fdb5::Key index_key({{"c", "3"}, {"d", "4"}});

        char data[] = "test";

        // archive

        /// DaosManager is configured with client config from the file
        fdb5::DaosStore dstore{db_key, config};
        fdb5::Store& store = dstore;
        std::unique_ptr<const fdb5::FieldLocation> loc(store.archive(index_key, data, sizeof(data)));
        /// @todo: two cont create with label happen here
        /// @todo: again, daos_fini happening before cont and pool close

        dstore.flush();  // not necessary for DAOS store

        // retrieve
        fdb5::Field field(std::move(loc), std::time(nullptr));
        std::cout << "Read location: " << field.location() << std::endl;
        std::unique_ptr<eckit::DataHandle> dh(store.retrieve(field));
        EXPECT(dynamic_cast<fdb5::DaosArrayPartHandle*>(dh.get()));

        eckit::MemoryHandle mh;
        dh->copyTo(mh);
        EXPECT(mh.size() == eckit::Length(sizeof(data)));
        EXPECT(::memcmp(mh.data(), data, sizeof(data)) == 0);
        /// @todo: again, daos_fini happening before

        // remove
        fdb5::DaosName field_name{field.location().uri()};
        fdb5::DaosName store_name{field_name.poolName(), field_name.containerName()};
        eckit::URI store_uri(store_name.URI());
        std::ostream out(std::cout.rdbuf());
        store.remove(store_uri, out, out, false);
        EXPECT(field_name.exists());
        store.remove(store_uri, out, out, true);
        EXPECT_NOT(field_name.exists());
        EXPECT_NOT(store_name.exists());

        /// @todo: test that when I write multiple things in the same pool, things work as expected

        /// @todo: check that the URI is properly produced

        /// @todo: assert a new DaosSession shows newly configured container_oids_per_alloc
    }

    SECTION("with POSIX Catalogue") {

        // FDB configuration

        std::string config_str{
            "spaces:\n"
            "- roots:\n"
            "  - path: " +
            store_tests_tmp_root().asString() +
            "\n"
            "schema : " +
            schema_file().path() +
            "\n"
            "daos:\n"
            "  store:\n"
            "    pool: " +
            pool_name +
            "\n"
            "  client:\n"
            "    container_oids_per_alloc: " +
            std::to_string(container_oids_per_alloc)};

        fdb5::Config config{YAMLConfiguration(config_str)};

        // schema

        fdb5::Schema schema{schema_file()};

        // request

        fdb5::Key request_key({{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"}, {"e", "5"}, {"f", "6"}});
        fdb5::Key db_key({{"a", "1"}, {"b", "2"}});
        fdb5::Key index_key({{"c", "3"}, {"d", "4"}});
        fdb5::Key field_key({{"e", "5"}, {"f", "6"}});

        // store data

        char data[] = "test";

        fdb5::DaosStore dstore{db_key, config};
        fdb5::Store& store = static_cast<fdb5::Store&>(dstore);
        std::unique_ptr<const fdb5::FieldLocation> loc(store.archive(index_key, data, sizeof(data)));
        /// @todo: there are two cont create with label here
        /// @todo: again, daos_fini happening before cont and pool close

        // index data

        {
            /// @todo: could have a unique ptr here, might not need a static cast
            fdb5::TocCatalogueWriter tcat{db_key, config};
            fdb5::Catalogue& cat = static_cast<fdb5::Catalogue&>(tcat);
            cat.deselectIndex();
            cat.selectIndex(index_key);
            // const fdb5::Index& idx = tcat.currentIndex();
            static_cast<fdb5::CatalogueWriter&>(tcat).archive(index_key, field_key, std::move(loc));

            /// flush store before flushing catalogue
            dstore.flush();  // not necessary if using a DAOS store
        }

        // find data

        fdb5::Field field;
        {
            fdb5::TocCatalogueReader tcat{db_key, config};
            fdb5::Catalogue& cat = static_cast<fdb5::Catalogue&>(tcat);
            cat.selectIndex(index_key);
            static_cast<fdb5::CatalogueReader&>(tcat).retrieve(field_key, field);
        }
        std::cout << "Read location: " << field.location() << std::endl;

        // retrieve data

        std::unique_ptr<eckit::DataHandle> dh(store.retrieve(field));
        EXPECT(dynamic_cast<fdb5::DaosArrayPartHandle*>(dh.get()));

        eckit::MemoryHandle mh;
        dh->copyTo(mh);
        EXPECT(mh.size() == eckit::Length(sizeof(data)));
        EXPECT(::memcmp(mh.data(), data, sizeof(data)) == 0);

        // remove data

        fdb5::DaosName field_name{field.location().uri()};
        fdb5::DaosName store_name{field_name.poolName(), field_name.containerName()};
        eckit::URI store_uri(store_name.URI());
        std::ostream out(std::cout.rdbuf());
        store.remove(store_uri, out, out, false);
        EXPECT(field_name.exists());
        store.remove(store_uri, out, out, true);
        EXPECT_NOT(field_name.exists());
        EXPECT_NOT(store_name.exists());

        // deindex data

        {
            fdb5::TocCatalogueWriter tcat{db_key, config};
            fdb5::Catalogue& cat        = static_cast<fdb5::Catalogue&>(tcat);
            metkit::mars::MarsRequest r = db_key.request("retrieve");
            eckit::Queue<fdb5::CatalogueWipeState> queue(100);
            fdb5::api::local::WipeCatalogueVisitor wv{queue, r, true};
            cat.visitEntries(wv, false);
        }

        /// @todo: again, daos_fini happening before
    }

    SECTION("VIA FDB API") {

        // FDB configuration

        int container_oids_per_alloc_small = 100;

        std::string config_str{
            "spaces:\n"
            "- roots:\n"
            "  - path: " +
            store_tests_tmp_root().asString() +
            "\n"
            "type: local\n"
            "schema : " +
            schema_file().path() +
            "\n"
            "engine: toc\n"
            "store: daos\n"
            "daos:\n"
            "  store:\n"
            "    pool: " +
            pool_name +
            "\n"
            "  client:\n"
            "    container_oids_per_alloc: " +
            std::to_string(container_oids_per_alloc_small)};

        fdb5::Config config{YAMLConfiguration(config_str)};

        // request

        fdb5::Key request_key({{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"}, {"e", "5"}, {"f", "6"}});
        fdb5::Key db_key({{"a", "1"}, {"b", "2"}});
        fdb5::Key index_key({{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"}});

        fdb5::FDBToolRequest full_req{request_key.request("retrieve"), false, std::vector<std::string>{"a", "b"}};
        fdb5::FDBToolRequest index_req{index_key.request("retrieve"), false, std::vector<std::string>{"a", "b"}};
        fdb5::FDBToolRequest db_req{db_key.request("retrieve"), false, std::vector<std::string>{"a", "b"}};

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
            info.print(std::cout, true, true, false, " ");
            std::cout << std::endl;
            ++count;
        }
        EXPECT(count == 0);
        std::cout << "Listed 0 fields" << std::endl;

        // store data

        char data[] = "test";

        /// @todo: here, DaosManager is being reconfigured with identical config, and it happens again multiple times
        /// below.
        //   Should this be avoided?
        fdb.archive(request_key, data, sizeof(data));
        std::cout << "Archived 1 field" << std::endl;

        fdb.flush();
        std::cout << "Flushed 1 field" << std::endl;

        // retrieve data

        metkit::mars::MarsRequest r = request_key.request("retrieve");
        std::unique_ptr<eckit::DataHandle> dh(fdb.retrieve(r));
        std::cout << "Retrieved 1 field location" << std::endl;

        eckit::MemoryHandle mh;
        dh->copyTo(mh);
        EXPECT(mh.size() == eckit::Length(sizeof(data)));
        EXPECT(::memcmp(mh.data(), data, sizeof(data)) == 0);

        // wipe data

        // dry run attempt to wipe with too specific request

        auto wipeObject = fdb.wipe(full_req);
        EXPECT(countWipeable(wipeObject) == 0);

        // dry run wipe index and store unit
        wipeObject = fdb.wipe(index_req);
        EXPECT(countWipeable(wipeObject) > 0);

        // dry run wipe database
        wipeObject = fdb.wipe(db_req);
        EXPECT(countWipeable(wipeObject) > 0);

        // ensure field still exists
        listObject = fdb.list(full_req);
        count      = 0;
        while (listObject.next(info)) {
            // info.print(std::cout, true, true);
            // std::cout << std::endl;
            count++;
        }
        EXPECT(count == 1);

        // attempt to wipe with too specific request
        wipeObject = fdb.wipe(full_req, true);
        EXPECT(countWipeable(wipeObject) == 0);
        /// @todo: really needed?
        fdb.flush();
        std::cout << "Flushed 0 fields" << std::endl;

        // wipe index and store unit (and DB container as there is only one index)
        wipeObject = fdb.wipe(index_req, true);
        EXPECT(countWipeable(wipeObject) > 0);
        std::cout << "Wiped 1 field" << std::endl;
        /// @todo: really needed?
        fdb.flush();
        std::cout << "Flushed 0 fields" << std::endl;

        // ensure field does not exist
        listObject = fdb.list(full_req);
        count      = 0;
        while (listObject.next(info))
            count++;
        EXPECT(count == 0);
        std::cout << "Listed 0 fields" << std::endl;

        /// @todo: ensure new DaosSession has updated daos client config
    }

    /// @todo: if doing what's in this section at the end of the previous section reusing the same FDB object,
    // archive() fails as it expects a toc file to exist, but it has been removed by previous wipe
    SECTION("FDB API RE-STORE AND WIPE DB") {

        // FDB configuration

        std::string config_str{
            "spaces:\n"
            "- roots:\n"
            "  - path: " +
            store_tests_tmp_root().asString() +
            "\n"
            "type: local\n"
            "schema : " +
            schema_file().path() +
            "\n"
            "engine: toc\n"
            "store: daos\n"
            "daos:\n"
            "  store:\n"
            "    pool: " +
            pool_name +
            "\n"
            "  client:\n"
            "    container_oids_per_alloc: " +
            std::to_string(container_oids_per_alloc)};

        fdb5::Config config{YAMLConfiguration(config_str)};

        // request

        fdb5::Key request_key({{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"}, {"e", "5"}, {"f", "6"}});
        fdb5::Key db_key({{"a", "1"}, {"b", "2"}});
        fdb5::Key index_key({{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"}});

        fdb5::FDBToolRequest full_req{request_key.request("retrieve"), false, std::vector<std::string>{"a", "b"}};
        fdb5::FDBToolRequest index_req{index_key.request("retrieve"), false, std::vector<std::string>{"a", "b"}};
        fdb5::FDBToolRequest db_req{db_key.request("retrieve"), false, std::vector<std::string>{"a", "b"}};

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
        EXPECT(countWipeable(wipeObject) > 0);
        /// @todo: really needed?
        fdb.flush();

        // ensure field does not exist

        fdb5::ListElement info;
        auto listObject = fdb.list(full_req);
        count           = 0;
        while (listObject.next(info)) {
            // info.print(std::cout, true, true);
            // std::cout << std::endl;
            count++;
        }
        EXPECT(count == 0);
    }

    // teardown daos

#ifdef fdb5_HAVE_DAOS_ADMIN
    /// AutoPoolDestroy is not possible here because the pool is
    /// created above with an ephemeral session
    fdb5::DaosSession().destroyPool(pool_uuid);
#else
    for (auto& c : fdb5::DaosSession().getPool(pool_uuid).listContainers())
        if (c == "1:2")
            fdb5::DaosSession().getPool(pool_uuid).destroyContainer(c);
#endif

    // remove root directory

    testing::deldir(store_tests_tmp_root());

    // remove dummy daos root

#ifdef fdb5_HAVE_DUMMY_DAOS
    testing::deldir(tmp_dummy_daos_root());
#endif
}

}  // namespace test
}  // namespace fdb

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
