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

#include "eckit/testing/Test.h"
#include "eckit/filesystem/URI.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/TmpFile.h"
#include "eckit/filesystem/TmpDir.h"
#include "eckit/io/FileHandle.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/config/YAMLConfiguration.h"

#include "fdb5/fdb5_config.h"
#include "fdb5/config/Config.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

#include "fdb5/toc/TocCatalogueWriter.h"
#include "fdb5/toc/TocCatalogueReader.h"

#include "fdb5/daos/DaosSession.h"
#include "fdb5/daos/DaosPool.h"
#include "fdb5/daos/DaosArrayHandle.h"

#include "fdb5/daos/DaosStore.h"
#include "fdb5/daos/DaosFieldLocation.h"
#include "fdb5/daos/DaosException.h"

using namespace eckit::testing;
using namespace eckit;

#ifdef fdb5_HAVE_DUMMY_DAOS
eckit::TmpDir& tmp_dummy_daos_root() {
    static eckit::TmpDir d{};
    return d;
}
#endif

// temporary schema,spaces,root files common to all DAOS Store tests

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

eckit::TmpDir& tmp_root() {
    static eckit::TmpDir d{};
    return d;
}

namespace fdb {
namespace test {

CASE( "Setup" ) {

#ifdef fdb5_HAVE_DUMMY_DAOS
    tmp_dummy_daos_root().mkdir();
    ::setenv("DUMMY_DAOS_DATA_ROOT", tmp_dummy_daos_root().path().c_str(), 1);
#endif

    // ensure fdb root directory exists. If not, then that root is 
    // registered as non existing and Store tests fail.
    tmp_root().mkdir();
    ::setenv("FDB_ROOT_DIRECTORY", tmp_root().path().c_str(), 1);

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

    std::string roots_str{tmp_root().asString() + " all yes yes"};

    std::unique_ptr<eckit::DataHandle> hr(roots_file().fileHandle());
    hr->openForWrite(roots_str.size());
    {
        eckit::AutoClose closer(*hr);
        hr->write(roots_str.data(), roots_str.size());
    }

    ::setenv("FDB_ROOTS_FILE", roots_file().path().c_str(), 1);

}

CASE("DaosStore tests") {

    // test parameters

    std::string pool_name{"fdb_pool"};
    int container_oids_per_alloc = 1000;

    // bootstrap daos

    uuid_t pool_uuid = {0};
    {
        fdb5::DaosManager::instance().configure(
            eckit::LocalConfiguration(YAMLConfiguration(
                "container_oids_per_alloc: " + std::to_string(container_oids_per_alloc)
            ))
        );
        fdb5::DaosSession s{};
        fdb5::DaosPool& pool = s.createPool(pool_name);
        pool.uuid(pool_uuid);
    }

    SECTION("archive and retrieve") {

        std::string config_str{
            "daos:\n"
            "  store:\n"
            "    pool: " + pool_name + "\n"
            "  client:\n"
            "    container_oids_per_alloc: " + std::to_string(container_oids_per_alloc)
        };

        fdb5::Config config{YAMLConfiguration(config_str)};

        fdb5::Schema schema{schema_file()};

        fdb5::Key request_key{"a=1,b=2,c=3,d=4,e=5,f=6"};
        fdb5::Key db_key{"a=1,b=2"};
        fdb5::Key index_key{"c=3,d=4"};

        char data[] = "test";

        // archive

        /// DaosManager is configured with client config from the file
        fdb5::DaosStore dstore{schema, db_key, config};
        fdb5::Store& store = dstore;
        std::unique_ptr<fdb5::FieldLocation> loc(store.archive(index_key, data, sizeof(data)));
        /// @todo: two cont create with label happen here
        /// @todo: again, daos_fini happening before cont and pool close

        dstore.flush();  // not necessary for DAOS store

        // retrieve
        fdb5::Field field(loc.get(), std::time(nullptr));
        std::cout << "Read location: " << field.location() << std::endl;
        std::unique_ptr<eckit::DataHandle> dh(store.retrieve(field));
        EXPECT(dynamic_cast<fdb5::DaosArrayHandle*>(dh.get()));
    
        eckit::MemoryHandle mh;
        dh->copyTo(mh);
        EXPECT(mh.size() == eckit::Length(sizeof(data)));
        EXPECT(::memcmp(mh.data(), data, sizeof(data)) == 0);
        /// @todo: again, daos_fini happening before

        // remove
        fdb5::DaosName field_name{field.location().uri()};
        fdb5::DaosName store_name{field_name.poolName(), field_name.contName()};
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

    SECTION("with Catalogue") {

        // FDB configuration

        std::string config_str{
            "schema : " + schema_file().path() + "\n"
            "daos:\n"
            "  store:\n"
            "    pool: " + pool_name + "\n"
            "  client:\n"
            "    container_oids_per_alloc: " + std::to_string(container_oids_per_alloc)
        };

        fdb5::Config config{YAMLConfiguration(config_str)};

        // schema

        fdb5::Schema schema{schema_file()};

        // request

        fdb5::Key request_key{"a=1,b=2,c=3,d=4,e=5,f=6"};
        fdb5::Key db_key{"a=1,b=2"};
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
            /// @todo: could have a unique ptr here, might not need a static cast
            fdb5::TocCatalogueWriter tcat{db_key, config};
            fdb5::Catalogue& cat = static_cast<fdb5::Catalogue&>(tcat);
            cat.deselectIndex();
            cat.selectIndex(index_key);
            //const fdb5::Index& idx = tcat.currentIndex();
            static_cast<fdb5::CatalogueWriter&>(tcat).archive(field_key, loc.get());

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
        EXPECT(dynamic_cast<fdb5::DaosArrayHandle*>(dh.get()));
    
        eckit::MemoryHandle mh;
        dh->copyTo(mh);
        EXPECT(mh.size() == eckit::Length(sizeof(data)));
        EXPECT(::memcmp(mh.data(), data, sizeof(data)) == 0);

        // remove data

        fdb5::DaosName field_name{field.location().uri()};
        fdb5::DaosName store_name{field_name.poolName(), field_name.contName()};
        eckit::URI store_uri(store_name.URI());
        std::ostream out(std::cout.rdbuf());
        store.remove(store_uri, out, out, false);
        EXPECT(field_name.exists());
        store.remove(store_uri, out, out, true);
        EXPECT_NOT(field_name.exists());
        EXPECT_NOT(store_name.exists());

        // deindex data

        /// @todo: the db_key.request passed to wipeVisitor is stored in the 
        ///        request_ member of WipeVisitor. On ngio, when that member is
        ///        accessed from TocWipeVisitor::visitDatabase a segfault occurs
        {
            fdb5::TocCatalogueWriter tcat{db_key, config};
            fdb5::Catalogue& cat = static_cast<fdb5::Catalogue&>(tcat);
            std::unique_ptr<fdb5::WipeVisitor> wv(cat.wipeVisitor(store, db_key.request("retrieve"), out, true, false, false));
            cat.visitEntries(*wv, store, false);
        }

        /// @todo: again, daos_fini happening before

    }

    SECTION("VIA FDB API") {

        // FDB configuration

        int container_oids_per_alloc_small = 100;

        std::string config_str{
            "type: local\n"
            "schema : " + schema_file().path() + "\n"
            "engine: toc\n"
            "store: daos\n"
            "daos:\n"
            "  store:\n"
            "    pool: " + pool_name + "\n"
            "  client:\n"
            "    container_oids_per_alloc: " + std::to_string(container_oids_per_alloc_small)
        };

        fdb5::Config config{YAMLConfiguration(config_str)};

        // request

        fdb5::Key request_key{"a=1,b=2,c=3,d=4,e=5,f=6"};
        fdb5::Key index_key{"a=1,b=2,c=3,d=4"};
        fdb5::Key db_key{"a=1,b=2"};

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

        std::unique_ptr<eckit::DataHandle> dh(fdb.retrieve(request_key.request("retrieve")));
    
        eckit::MemoryHandle mh;
        dh->copyTo(mh);
        EXPECT(mh.size() == eckit::Length(sizeof(data)));
        EXPECT(::memcmp(mh.data(), data, sizeof(data)) == 0);

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

        /// @todo: ensure index and corresponding container do not exist
        /// @todo: ensure DB still exists
        /// @todo: list db or index and expect count = 0?

        /// @todo: ensure new DaosSession has updated daos client config

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

        fdb5::Key request_key{"a=1,b=2,c=3,d=4,e=5,f=6"};
        fdb5::Key index_key{"a=1,b=2,c=3,d=4"};
        fdb5::Key db_key{"a=1,b=2"};

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

    /// AutoPoolDestroy is not possible here because the pool is 
    /// created above with an ephemeral session
    fdb5::DaosSession().getPool(pool_uuid, pool_name).destroy();

}

}  // namespace test
}  // namespace fdb

int main(int argc, char **argv)
{
    return run_tests ( argc, argv );
}
