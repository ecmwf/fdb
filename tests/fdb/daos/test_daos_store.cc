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
#include "eckit/filesystem/TmpFile.h"
#include "eckit/io/FileHandle.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/config/YAMLConfiguration.h"

#include "fdb5/daos/DaosSession.h"
#include "fdb5/daos/DaosPool.h"
#include "fdb5/daos/DaosHandle.h"

#include "fdb5/daos/DaosStore.h"
#include "fdb5/daos/DaosFieldLocation.h"

#include "fdb5/toc/TocCatalogueWriter.h"
#include "fdb5/toc/TocCatalogueReader.h"

using namespace eckit::testing;
using namespace eckit;

namespace fdb {
namespace test {

CASE("DAOS STORE") {

    SECTION("ARCHIVE AND RETRIEVE") {

        std::string pool_name{"test_pool"};

        std::string config_str{
            "store: daos\n"
            "daos:\n"
            "  pool: " + pool_name + "\n"
            "  test_config_item: 1"
        };

        eckit::TmpFile config_file{};
        std::unique_ptr<eckit::DataHandle> h(config_file.fileHandle());
        h->openForWrite(config_str.size());
        {
            eckit::AutoClose closer(*h);
            h->write(config_str.data(), config_str.size());
        }

        fdb5::Config config{YAMLConfiguration(config_file)};
        config.set("configSource", config_file);

        std::string schema_str{"[ a, b [ c, d [ e, f ]]]"};

        eckit::TmpFile schema_file{};
        std::unique_ptr<eckit::DataHandle> hs(schema_file.fileHandle());
        hs->openForWrite(schema_str.size());
        {
            eckit::AutoClose closer(*hs);
            hs->write(schema_str.data(), schema_str.size());
        }

        fdb5::Schema schema{schema_file};

        fdb5::Key request_key{"a=1,b=2,c=3,d=4,e=5,f=6"};
        fdb5::Key db_key{"a=1,b=2"};
        fdb5::Key index_key{"c=3,d=4"};

        char data[] = "test";

        uuid_t pool_uuid = {0};
        {
            // TODO: pass in DAOS config from FDB. Probably need to issue some FDB call which 
            // populates config in DaosManager.
            fdb5::DaosSession s{};
            fdb5::DaosPool& pool = s.createPool(pool_name);
            pool.uuid(pool_uuid);
        }

        // archive
        fdb5::DaosStore dstore{schema, db_key, config};
        fdb5::Store& store = dstore;
        std::unique_ptr<fdb5::FieldLocation> loc(store.archive(index_key, data, sizeof(data)));
        // TODO: there are two cont create with label here
        // TODO: again, daos_fini happening before cont and pool close

        // retrieve
        fdb5::Field field(loc.get(), std::time(nullptr));
        std::cout << "Read location: " << field.location() << std::endl;
        std::unique_ptr<eckit::DataHandle> dh(store.retrieve(field));
        EXPECT(dynamic_cast<fdb5::DaosHandle*>(dh.get()));
    
        eckit::MemoryHandle mh;
        dh->copyTo(mh);
        EXPECT(mh.size() == eckit::Length(sizeof(data)));
        EXPECT(::memcmp(mh.data(), data, sizeof(data)) == 0);
        dh.reset();
        // TODO: again, daos_fini happening before

        // test that when I write multiple things in the same pool, things work as expected
        // check that the URI is properly produced
        // test using DaosStore together with TocCatlogue
        // implement wipe

        fdb5::DaosSession().getPool(pool_uuid, pool_name).destroy();

    }

    SECTION("WITH CATALOGUE") {

        // test parameters

        std::string pool_name{"test_pool"};

        // bootstrap daos

        uuid_t pool_uuid = {0};
        {
            // TODO: pass in DAOS config from FDB. Probably need to issue some FDB call which 
            // populates config in DaosManager.
            fdb5::DaosSession s{};
            fdb5::DaosPool& pool = s.createPool(pool_name);
            pool.uuid(pool_uuid);
        }

        // FDB configuration

        // TODO: engine and store are not required unless using the factories?
        std::string config_str{
            "engine: toc\n"
            "store: daos\n"
            "daos:\n"
            "  pool: " + pool_name + "\n"
            "  test_config_item: 1"
        };

        eckit::TmpFile config_file{};
        std::unique_ptr<eckit::DataHandle> h(config_file.fileHandle());
        h->openForWrite(config_str.size());
        {
            eckit::AutoClose closer(*h);
            h->write(config_str.data(), config_str.size());
        }

        fdb5::Config config{YAMLConfiguration(config_file)};
        config.set("configSource", config_file);

        // schema

        std::string schema_str{"[ a, b [ c, d [ e, f ]]]"};

        eckit::TmpFile schema_file{};
        std::unique_ptr<eckit::DataHandle> hs(schema_file.fileHandle());
        hs->openForWrite(schema_str.size());
        {
            eckit::AutoClose closer(*hs);
            hs->write(schema_str.data(), schema_str.size());
        }

        fdb5::Schema schema{schema_file};

        // request

        fdb5::Key request_key{"a=1,b=2,c=3,d=4,e=5,f=6"};
        fdb5::Key db_key{"a=1,b=2"};
        fdb5::Key index_key{"c=3,d=4"};
        fdb5::Key field_key{"e=5,f=6"};

        // store data

        char data[] = "test";

        fdb5::DaosStore dstore{schema, db_key, config};
        fdb5::Store& store = dynamic_cast<fdb5::Store&>(dstore);
        std::unique_ptr<fdb5::FieldLocation> loc(store.archive(index_key, data, sizeof(data)));
        // TODO: there are two cont create with label here
        // TODO: again, daos_fini happening before cont and pool close

        // index data

        {
            fdb5::TocCatalogueWriter tcat{db_key, config};
            fdb5::Catalogue& cat = dynamic_cast<fdb5::Catalogue&>(tcat);
            cat.deselectIndex();
            cat.selectIndex(index_key);
            // const fdb5::Index& idx = tcat.currentIndex();
            dynamic_cast<fdb5::CatalogueWriter&>(tcat).archive(field_key, loc.get());
        }

        // deindex data

        fdb5::Field field;
        {
            fdb5::TocCatalogueReader tcat{db_key, config};
            fdb5::Catalogue& cat = dynamic_cast<fdb5::Catalogue&>(tcat);
            cat.selectIndex(index_key);
            dynamic_cast<fdb5::CatalogueReader&>(tcat).retrieve(field_key, field);
        }
        std::cout << "Read location: " << field.location() << std::endl;

        // retrieve data

        std::unique_ptr<eckit::DataHandle> dh(store.retrieve(field));
        EXPECT(dynamic_cast<fdb5::DaosHandle*>(dh.get()));
    
        eckit::MemoryHandle mh;
        dh->copyTo(mh);
        EXPECT(mh.size() == eckit::Length(sizeof(data)));
        EXPECT(::memcmp(mh.data(), data, sizeof(data)) == 0);
        dh.reset();
        // TODO: again, daos_fini happening before

        // teardown daos

        fdb5::DaosSession().getPool(pool_uuid, pool_name).destroy();

    }

}

}  // namespace test
}  // namespace fdb

int main(int argc, char **argv)
{
    return run_tests ( argc, argv );
}