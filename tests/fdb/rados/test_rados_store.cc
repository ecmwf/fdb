/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Engine.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Store.h"
#include "fdb5/fdb5_config.h"
#include "fdb5/rados/RadosFieldLocation.h"
#include "fdb5/rados/RadosStore.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/toc/TocCatalogueReader.h"
#include "fdb5/toc/TocCatalogueWriter.h"

#include "metkit/mars/MarsRequest.h"

#include "eckit/config/Resource.h"
#include "eckit/config/YAMLConfiguration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/TmpFile.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/io/Offset.h"
#include "eckit/io/PartHandle.h"
#include "eckit/io/rados/RadosCluster.h"
#include "eckit/io/rados/RadosNamespace.h"
#include "eckit/io/rados/RadosObject.h"
#include "eckit/io/rados/RadosPool.h"
#include "eckit/log/Log.h"
#include "eckit/testing/Test.h"

#include <cstddef>
#include <cstring>
#include <ctime>
#include <iostream>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

using namespace eckit;

//----------------------------------------------------------------------------------------------------------------------

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

#ifdef fdb5_HAVE_RADOS_TESTS_MANAGE_POOLS
void ensureCleanPools(const std::string& prefix) {
    ASSERT(prefix.length() > 3);
    for (const std::string& name : eckit::RadosCluster::instance().listPools()) {
        if (name.rfind(prefix, 0) == 0) {
            eckit::RadosPool{name}.destroy();
        }
    }
}
#endif

}  // namespace

eckit::TmpFile& schema_file() {
    static eckit::TmpFile f{};
    return f;
}

eckit::PathName& store_tests_tmp_root() {
    static eckit::PathName sd("./rados_store_tests_fdb_root");
    return sd;
}

void cleanupRados() noexcept {
    try {
#ifdef fdb5_HAVE_RADOS_TESTS_MANAGE_POOLS
        ensureCleanPools("test-store");
#else
        const std::string pool = eckit::Resource<std::string>("fdbRadosTestPool;$FDB_RADOS_TEST_POOL", "");
        for (const std::string& prefix : {"test-store1", "test-store2", "test-store3", "test-store4"}) {
            ensureCleanNamespaces(pool, prefix);
        }
#endif
        if (store_tests_tmp_root().exists()) {
            deldir(store_tests_tmp_root());
        }
    }
    catch (...) {
        eckit::Log::error() << "FDB RADOS store cleanup failed" << std::endl;
    }
}

/// @note: counts only the URIs that would actually be deleted, filtering out purely
///   informational wipe elements (safe/info/error) so a too-specific request yields 0.
size_t countWipeable(fdb5::WipeIterator& wipeObject, bool print = true) {
    size_t count = 0;
    fdb5::WipeElement elem;
    while (wipeObject.next(elem)) {
        if (print) {
            std::cout << elem << std::endl;
        }
        if (elem.type() != fdb5::WipeElementType::ERROR && elem.type() != fdb5::WipeElementType::CATALOGUE_INFO &&
            elem.type() != fdb5::WipeElementType::CATALOGUE_SAFE && elem.type() != fdb5::WipeElementType::STORE_SAFE) {
            count += elem.uris().size();
        }
    }
    return count;
}

//----------------------------------------------------------------------------------------------------------------------

namespace fdb::test {

CASE("Setup") {

    // ensure fdb root directory exists. If not, then that root is
    // registered as non existing and Store tests fail.
    if (store_tests_tmp_root().exists()) {
        deldir(store_tests_tmp_root());
    }
    store_tests_tmp_root().mkdir();
    ::setenv("FDB_ROOT_DIRECTORY", store_tests_tmp_root().path().c_str(), 1);

    // prepare schema for tests involving S3Store

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
}

CASE("RadosStore tests") {

    SECTION("archive and retrieve") {

        std::string test_id = "test-store1";
#ifdef eckit_HAVE_RADOS_TESTS_MANAGE_POOLS
        std::string pool = test_id;
        eckit::RadosPool{pool}.ensureDestroyed();
        eckit::RadosPool{pool}.ensureCreated();  /// @todo: auto pool destroyer
#else
        std::string pool;
        pool = eckit::Resource<std::string>("fdbRadosTestPool;$FDB_RADOS_TEST_POOL", pool);
        EXPECT(pool.length() > 0);
        ensureCleanNamespaces(pool, test_id);
#endif
        std::string config_str{
            "spaces:\n"
            "- roots:\n"
            "  - path: " +
            store_tests_tmp_root().asString() +
            "\n"
            "    pool: " +
            pool +
            "\n"
            "    root_namespace: " +
            test_id +
            "_root\n"
            "    namespace_prefix: " +
            test_id +
            "\n"
            "rados:\n"
            "  maxPartSize: 16\n"
            "  store:\n"
            "    pool: " +
            pool +
            "\n"
            "    root_namespace: " +
            test_id +
            "_root\n"
            "    namespace_prefix: " +
            test_id + "\n"};

        fdb5::Config config{YAMLConfiguration(config_str)};

        fdb5::Schema schema{schema_file()};

        fdb5::Key request_key({{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"}, {"e", "5"}, {"f", "6"}});
        fdb5::Key db_key({{"a", "1"}, {"b", "2"}});
        fdb5::Key index_key({{"c", "3"}, {"d", "4"}});

        const std::string data{"0123456789abcdef0123456789abcdef"};

        // archive

        fdb5::RadosStore rados_store{schema, db_key, config};
        fdb5::Store& store = rados_store;
        std::unique_ptr<const fdb5::FieldLocation> loc(store.archive(index_key, data.data(), data.size()));

        rados_store.close();

        // retrieve
        fdb5::Field field(std::move(loc), std::time(nullptr));
        std::cout << "Read location: " << field.location() << std::endl;
        std::unique_ptr<eckit::DataHandle> dh(store.retrieve(field));
        /// @note: the field spans potentially several objects and is returned as an
        ///   eckit::PartHandle wrapping a RadosMultiObjReadHandle.
        EXPECT(dynamic_cast<eckit::PartHandle*>(dh.get()));

        eckit::MemoryHandle mh;
        dh->copyTo(mh);
        EXPECT(mh.size() == eckit::Length(data.size()));
        EXPECT(::memcmp(mh.data(), data.data(), data.size()) == 0);

        // remove
        eckit::RadosObject field_name{field.location().uri()};
        eckit::RadosNamespace store_name = field_name.nspace();
        eckit::URI store_uri(store_name.uri());
        std::ostream out(std::cout.rdbuf());
        store.remove(store_uri, out, out, false);
        EXPECT(field_name.exists());
        store.remove(store_uri, out, out, true);
        EXPECT_NOT(field_name.exists());
        EXPECT(store_name.listObjects().size() == 0);

        std::unique_ptr<const fdb5::FieldLocation> expiring_location;
        {
            fdb5::RadosStore expiring_store{schema, db_key, config};
            fdb5::Store& store = expiring_store;
            expiring_location = store.archive(index_key, data.data(), data.size());
        }

        fdb5::Field expiring_field(std::move(expiring_location), std::time(nullptr));
        std::unique_ptr<eckit::DataHandle> expiring_handle(expiring_field.dataHandle());
        eckit::MemoryHandle expiring_data;
        expiring_handle->copyTo(expiring_data);
        EXPECT(expiring_data.size() == eckit::Length(data.size()));
        EXPECT(::memcmp(expiring_data.data(), data.data(), data.size()) == 0);

        eckit::RadosObject{expiring_field.location().uri()}.nspace().destroy();
    }

    SECTION("rejects namespace prefixes containing underscores") {

        fdb5::Schema schema{schema_file()};
        fdb5::Key db_key({{"a", "1"}, {"b", "2"}});

        std::string config_str{
            "spaces:\n"
            "- roots:\n"
            "  - path: unused\n"
            "    pool: unused\n"
            "    root_namespace: unused\n"
            "    namespace_prefix: invalid_prefix\n"};

        fdb5::Config config{YAMLConfiguration(config_str)};
        EXPECT_THROWS_AS((fdb5::RadosStore{schema, db_key, config}), eckit::UserError);
        EXPECT_THROWS_AS((fdb5::Engine::backend("rados").location(db_key, config)), eckit::UserError);
    }

    SECTION("RadosFieldLocation three-argument constructor forwards an empty remapKey") {
        fdb5::RadosFieldLocation loc{eckit::URI{"rados", "pool/ns/obj"}, eckit::Offset(0), eckit::Length(1)};
        EXPECT(loc.remapKey().empty());
        EXPECT(loc.uri().name() == "pool/ns/obj");
        EXPECT(loc.offset() == eckit::Offset(0));
        EXPECT(loc.length() == eckit::Length(1));
    }

    SECTION("with POSIX Catalogue") {

        std::string test_id = "test-store2";
#ifdef eckit_HAVE_RADOS_TESTS_MANAGE_POOLS
        std::string pool = test_id;
        eckit::RadosPool{pool}.ensureDestroyed();
        eckit::RadosPool{pool}.ensureCreated();  /// @todo: auto pool destroyer
#else
        std::string pool;
        pool = eckit::Resource<std::string>("fdbRadosTestPool;$FDB_RADOS_TEST_POOL", pool);
        EXPECT(pool.length() > 0);
        ensureCleanNamespaces(pool, test_id);
#endif
        std::string config_str{
            "spaces:\n"
            "- roots:\n"
            "  - path: " +
            store_tests_tmp_root().asString() +
            "\n"
            "    pool: " +
            pool +
            "\n"
            "    root_namespace: " +
            test_id +
            "_root\n"
            "    namespace_prefix: " +
            test_id +
            "\n"
            "schema : " +
            schema_file().path() +
            "\n"
            "rados:\n"
            "  store:\n"
            "    pool: " +
            pool +
            "\n"
            "    root_namespace: " +
            test_id +
            "_root\n"
            "    namespace_prefix: " +
            test_id + "\n"};

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

        fdb5::RadosStore rados_store{schema, db_key, config};
        fdb5::Store& store = static_cast<fdb5::Store&>(rados_store);
        std::unique_ptr<const fdb5::FieldLocation> loc(store.archive(index_key, data, sizeof(data)));

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
            rados_store.flush();
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
        /// @note: the field spans potentially several objects and is returned as an
        ///   eckit::PartHandle wrapping a RadosMultiObjReadHandle.
        EXPECT(dynamic_cast<eckit::PartHandle*>(dh.get()));

        eckit::MemoryHandle mh;
        dh->copyTo(mh);
        EXPECT(mh.size() == eckit::Length(sizeof(data)));
        EXPECT(::memcmp(mh.data(), data, sizeof(data)) == 0);

        // remove data
        eckit::RadosObject field_name{field.location().uri()};
        eckit::RadosNamespace store_name{field_name.nspace()};
        eckit::URI store_uri(store_name.uri());
        std::ostream out(std::cout.rdbuf());
        store.remove(store_uri, out, out, false);
        EXPECT(field_name.exists());
        store.remove(store_uri, out, out, true);
        EXPECT_NOT(field_name.exists());
        EXPECT(store_name.listObjects().size() == 0);
    }

    SECTION("VIA FDB API") {

        std::string test_id = "test-store3";

        /// @note: the POSIX toc catalogue root is shared across sections; reset it so this
        ///   section is not polluted by entries left behind by previous sections.
        if (store_tests_tmp_root().exists()) {
            deldir(store_tests_tmp_root());
        }
        store_tests_tmp_root().mkdir();
#ifdef eckit_HAVE_RADOS_TESTS_MANAGE_POOLS
        std::string pool = test_id;
        eckit::RadosPool{pool}.ensureDestroyed();
        eckit::RadosPool{pool}.ensureCreated();  /// @todo: auto pool destroyer
#else
        std::string pool;
        pool = eckit::Resource<std::string>("fdbRadosTestPool;$FDB_RADOS_TEST_POOL", pool);
        EXPECT(pool.length() > 0);
        ensureCleanNamespaces(pool, test_id);
#endif

        std::string config_str{
            "spaces:\n"
            "- roots:\n"
            "  - path: " +
            store_tests_tmp_root().asString() +
            "\n"
            "    pool: " +
            pool +
            "\n"
            "    root_namespace: " +
            test_id +
            "_root\n"
            "    namespace_prefix: " +
            test_id +
            "\n"
            "type: local\n"
            "schema : " +
            schema_file().path() +
            "\n"
            "engine: toc\n"
            "store: rados\n"
            "rados:\n"};

        config_str += "  maxPartSize: 16\n";

        config_str += "  store:\n";

        config_str += "    pool: " + pool +
                      "\n"
                      "    root_namespace: " +
                      test_id +
                      "_root\n"
                      "    namespace_prefix: " +
                      test_id + "\n";

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

        auto listObject = fdb.list(db_req);

        count = 0;
        while (listObject.next(info)) {
            info.print(std::cout, true, true, false, " ");
            std::cout << std::endl;
            ++count;
        }
        EXPECT(count == 0);

        // store data

        char data[] = "test123456";

        /// @note: maxPartSize is set to 16, and four 10-byte fields are archived, spanning 3 objects
        for (int i = 0; i < 4; i++) {
            std::cout << "Archive field " << i << std::endl;
            fdb5::Key request_key_i(
                {{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"}, {"e", "5"}, {"f", std::to_string(6 + i)}});
            fdb.archive(request_key_i, data, sizeof(data));
        }

        fdb.flush();

        // retrieve data

        for (int i = 0; i < 4; i++) {
            std::cout << "Retrieve field " << i << std::endl;
            fdb5::Key request_key_i(
                {{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"}, {"e", "5"}, {"f", std::to_string(6 + i)}});
            metkit::mars::MarsRequest r_i = request_key_i.request("retrieve");
            std::unique_ptr<eckit::DataHandle> dh(fdb.retrieve(r_i));

            eckit::MemoryHandle mh;
            dh->copyTo(mh);
            EXPECT(mh.size() == eckit::Length(sizeof(data)));
            EXPECT(::memcmp(mh.data(), data, sizeof(data)) == 0);
        }

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
        count = 0;
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

        // wipe index and store unit (and DB pool or namespace as there is only one index)
        wipeObject = fdb.wipe(index_req, true);
        EXPECT(countWipeable(wipeObject) > 0);
        /// @todo: really needed?
        fdb.flush();

        // ensure field does not exist
        listObject = fdb.list(full_req);
        count = 0;
        while (listObject.next(info)) {
            count++;
        }
        EXPECT(count == 0);
    }

    /// @todo: if doing what's in this section at the end of the previous section reusing the same FDB object,
    // archive() fails as it expects a toc file to exist, but it has been removed by previous wipe
    SECTION("FDB API RE-STORE AND WIPE DB") {

        std::string test_id = "test-store4";

        /// @note: the POSIX toc catalogue root is shared across sections; reset it so this
        ///   section is not polluted by entries left behind by previous sections.
        if (store_tests_tmp_root().exists()) {
            deldir(store_tests_tmp_root());
        }
        store_tests_tmp_root().mkdir();
#ifdef eckit_HAVE_RADOS_TESTS_MANAGE_POOLS
        std::string pool = test_id;
        eckit::RadosPool{pool}.ensureDestroyed();
        eckit::RadosPool{pool}.ensureCreated();  /// @todo: auto pool destroyer
#else
        std::string pool;
        pool = eckit::Resource<std::string>("fdbRadosTestPool;$FDB_RADOS_TEST_POOL", pool);
        EXPECT(pool.length() > 0);
        ensureCleanNamespaces(pool, test_id);
#endif

        std::string config_str{
            "spaces:\n"
            "- roots:\n"
            "  - path: " +
            store_tests_tmp_root().asString() +
            "\n"
            "    pool: " +
            pool +
            "\n"
            "    root_namespace: " +
            test_id +
            "_root\n"
            "    namespace_prefix: " +
            test_id +
            "\n"
            "type: local\n"
            "schema : " +
            schema_file().path() +
            "\n"
            "engine: toc\n"
            "store: rados\n"
            "rados:\n"};

        config_str += "  maxPartSize: 16\n";

        config_str += "  store:\n";

        config_str += "    pool: " + pool +
                      "\n"
                      "    root_namespace: " +
                      test_id +
                      "_root\n"
                      "    namespace_prefix: " +
                      test_id + "\n";

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

        auto wipeObject = fdb.wipe(db_req, true);
        EXPECT(countWipeable(wipeObject) > 0);
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
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb::test


int main(int argc, char** argv) {

    int ret = -1;
    try {
        ret = eckit::testing::run_tests(argc, argv);
    }
    catch (...) {
        eckit::Log::error() << "FDB RADOS store tests terminated with an exception" << std::endl;
    }

    cleanupRados();

    return ret;
}
