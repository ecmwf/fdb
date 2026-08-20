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
#include "fdb5/config/Config.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/DatabaseNotFoundException.h"
#include "fdb5/database/DbStats.h"
#include "fdb5/database/Engine.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/rados/RadosCatalogueReader.h"
#include "fdb5/rados/RadosCatalogueWriter.h"
#include "fdb5/rados/RadosFieldLocation.h"
#include "fdb5/rados/RadosStore.h"
#include "fdb5/rules/Schema.h"

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
#include "eckit/io/rados/RadosPool.h"
#include "eckit/log/Log.h"
#include "eckit/testing/Test.h"

#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <ostream>
#include <sstream>
#include <string>
#include <thread>
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

// Guard against clobbering unrelated namespaces in a shared CI pool.
void ensureCleanNamespaces(const std::string& pool, const std::string& prefix) {
    ASSERT(prefix.length() > 3);
    for (const std::string& name : eckit::RadosCluster::instance().listNamespaces(pool)) {
        if (name.rfind(prefix, 0) == 0) {
            eckit::RadosNamespace{pool, name}.destroy();
        }
    }
}

// Count only URIs that would actually be deleted, filtering out safe/info/error records so that
// too-specific requests yield zero.
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

// temporary schema,spaces,root files common to all RADOS Catalogue tests

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

void cleanupRados() noexcept {
    try {
#ifdef eckit_HAVE_RADOS_TESTS_MANAGE_POOLS
        eckit::RadosPool{"test-catalogue"}.ensureDestroyed();
#else
        ensureCleanNamespaces(eckit::Resource<std::string>("fdbRadosTestPool;$FDB_RADOS_TEST_POOL", ""),
                              "test-catalogue");
#endif
        if (catalogue_tests_tmp_root().exists()) {
            deldir(catalogue_tests_tmp_root());
        }
    }
    catch (...) {
        eckit::Log::error() << "FDB RADOS catalogue cleanup failed" << std::endl;
    }
}

}  // namespace

namespace fdb::test {

CASE("Setup") {

    // ensure fdb root directory exists. If not, then that root is
    // registered as non existing and Catalogue/Store tests fail.
    if (catalogue_tests_tmp_root().exists()) {
        deldir(catalogue_tests_tmp_root());
    }
    catalogue_tests_tmp_root().mkdir();
    ::setenv("FDB_ROOT_DIRECTORY", catalogue_tests_tmp_root().path().c_str(), 1);

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

    SECTION("RadosCatalogue archive (index) and retrieve without a Store") {

        std::string config_str{
            "spaces:\n"
            "- roots:\n"
            "  - path: " +
            catalogue_tests_tmp_root().asString() +
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
            "  catalogue:\n"
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

        /// @note: a=11,b=22 instead of a=1,b=2 to avoid collision with potential parallel runs of store tests using
        /// a=1,b=2
        fdb5::Key request_key({{"a", "11"}, {"b", "22"}, {"c", "3"}, {"d", "4"}, {"e", "5"}, {"f", "6"}});
        fdb5::Key db_key({{"a", "11"}, {"b", "22"}});
        fdb5::Key index_key({{"c", "3"}, {"d", "4"}});
        fdb5::Key field_key({{"e", "5"}, {"f", "6"}});

        // archive

        std::unique_ptr<fdb5::FieldLocation> loc(
            new fdb5::RadosFieldLocation(eckit::URI{"rados", "test_uri"}, eckit::Offset(0), eckit::Length(1)));

        eckit::URI catalogue_uri;
        {
            fdb5::RadosCatalogueWriter dcatw{db_key, config};

            fdb5::Catalogue& cat = dcatw;
            cat.selectIndex(index_key);

            fdb5::CatalogueWriter& catw = dcatw;
            catw.archive(index_key, field_key, std::move(loc));
            cat.flush(0);
            catalogue_uri = cat.uri();
        }

        {
            auto reopened = fdb5::CatalogueWriterFactory::instance().build(catalogue_uri, config);
            EXPECT(reopened->key() == db_key);
            EXPECT_NOT(reopened->schema().empty());
        }

        // retrieve

        {
            fdb5::RadosCatalogueReader dcatr{db_key, config};

            fdb5::Catalogue& cat = dcatr;
            EXPECT(cat.selectIndex(index_key));

            fdb5::Key missing_index_key({{"c", "missing"}, {"d", "missing"}});
            EXPECT_NOT(cat.selectIndex(missing_index_key));
            EXPECT_NOT(cat.selectIndex(missing_index_key));

            EXPECT(cat.selectIndex(index_key));
            cat.deselectIndex();
            EXPECT(cat.selectIndex(index_key));

            fdb5::Field f;
            fdb5::CatalogueReader& catr = dcatr;
            catr.retrieve(field_key, f);
            EXPECT(f.location().uri().name() == eckit::URI("rados", "test_uri").name());
            EXPECT(f.location().offset() == eckit::Offset(0));
            EXPECT(f.location().length() == eckit::Length(1));
        }
    }

    SECTION("RadosCatalogue archive (index) and retrieve with a RadosStore") {

        // FDB configuration

        std::string config_str{
            "spaces:\n"
            "- roots:\n"
            "  - path: " +
            catalogue_tests_tmp_root().asString() +
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
            "  pool: " +
            pool +
            "\n"
            "  root_namespace: " +
            test_id +
            "_root\n"
            "  namespace_prefix: " +
            test_id + "\n"};

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
        std::unique_ptr<const fdb5::FieldLocation> loc(store.archive(index_key, data, sizeof(data)));

        // index data

        {
            fdb5::RadosCatalogueWriter rcatw{db_key, config};
            fdb5::Catalogue& cat = rcatw;
            cat.deselectIndex();
            cat.selectIndex(index_key);
            fdb5::CatalogueWriter& catw = rcatw;
            catw.archive(index_key, field_key, std::move(loc));

            /// flush store before flushing catalogue
            rstore.flush();  // not necessary if using a RADOS store
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
        /// @note: the field spans potentially several objects and is returned as an
        ///   eckit::PartHandle wrapping a RadosMultiObjReadHandle.
        EXPECT(dynamic_cast<eckit::PartHandle*>(dh.get()));

        eckit::MemoryHandle mh;
        dh->copyTo(mh);
        EXPECT(mh.size() == eckit::Length(sizeof(data)));
        EXPECT(::memcmp(mh.data(), data, sizeof(data)) == 0);
    }

    SECTION("RadosCatalogue reports missing databases via factory paths") {

        std::string config_str{
            "spaces:\n"
            "- roots:\n"
            "  - path: " +
            catalogue_tests_tmp_root().asString() +
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
            "  catalogue:\n"
            "    pool: " +
            pool +
            "\n"
            "    root_namespace: " +
            test_id +
            "_root\n"
            "    namespace_prefix: " +
            test_id + "\n"};

        fdb5::Config config{YAMLConfiguration(config_str)};

        // A key-based reader over a DB that was never written must fail to open.
        fdb5::Key missing_db_key({{"a", "99"}, {"b", "99"}});
        {
            fdb5::RadosCatalogueReader reader{missing_db_key, config};
            fdb5::CatalogueReader& cr = reader;
            EXPECT_NOT(cr.open());
        }

        // A URI-based reader/writer over a DB whose namespace has no catalogue KV must throw
        // DatabaseNotFoundException at construction so the caller does not proceed on empty state.
        const std::string missing_ns = test_id + "_" + missing_db_key.valuesToString();
        const eckit::URI missing_uri = eckit::RadosKeyValue{pool, missing_ns, "catalogue_kv"}.uri();

        EXPECT_THROWS_AS(fdb5::CatalogueReaderFactory::instance().build(missing_uri, config),
                         fdb5::DatabaseNotFoundException);
        EXPECT_THROWS_AS(fdb5::CatalogueWriterFactory::instance().build(missing_uri, config),
                         fdb5::DatabaseNotFoundException);
    }

    SECTION("RadosCatalogueReader::stats reports index and field counts") {

        std::string config_str{
            "spaces:\n"
            "- roots:\n"
            "  - path: " +
            catalogue_tests_tmp_root().asString() +
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
            "  catalogue:\n"
            "    pool: " +
            pool +
            "\n"
            "    root_namespace: " +
            test_id +
            "_root\n"
            "    namespace_prefix: " +
            test_id + "\n"};

        fdb5::Config config{YAMLConfiguration(config_str)};

        fdb5::Key db_key({{"a", "77"}, {"b", "77"}});
        fdb5::Key index_key({{"c", "3"}, {"d", "4"}});
        fdb5::Key field_key_1({{"e", "5"}, {"f", "6"}});
        fdb5::Key field_key_2({{"e", "5"}, {"f", "7"}});

        {
            fdb5::RadosCatalogueWriter writer{db_key, config};
            fdb5::Catalogue& cat = writer;
            cat.selectIndex(index_key);
            std::unique_ptr<fdb5::FieldLocation> loc1(
                new fdb5::RadosFieldLocation(eckit::URI{"rados", "unused"}, eckit::Offset(0), eckit::Length(1)));
            std::unique_ptr<fdb5::FieldLocation> loc2(
                new fdb5::RadosFieldLocation(eckit::URI{"rados", "unused"}, eckit::Offset(1), eckit::Length(1)));
            static_cast<fdb5::CatalogueWriter&>(writer).archive(index_key, field_key_1, std::move(loc1));
            static_cast<fdb5::CatalogueWriter&>(writer).archive(index_key, field_key_2, std::move(loc2));
            cat.flush(0);
        }

        {
            fdb5::RadosCatalogueReader reader{db_key, config};
            fdb5::CatalogueReader& cr = reader;
            EXPECT(cr.open());
            fdb5::DbStats stats = cr.stats();
            std::ostringstream oss;
            stats.report(oss);
            const std::string report = oss.str();
            // Must expose non-empty output rather than throwing NOTIMP.
            EXPECT(!report.empty());
            EXPECT(report.find("Indexes") != std::string::npos);
            EXPECT(report.find("Fields") != std::string::npos);
        }
    }

    SECTION("RadosCatalogue persists ControlIdentifiers across processes") {

        std::string config_str{
            "spaces:\n"
            "- roots:\n"
            "  - path: " +
            catalogue_tests_tmp_root().asString() +
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
            "  catalogue:\n"
            "    pool: " +
            pool +
            "\n"
            "    root_namespace: " +
            test_id +
            "_root\n"
            "    namespace_prefix: " +
            test_id + "\n"};

        fdb5::Config config{YAMLConfiguration(config_str)};

        fdb5::Key db_key({{"a", "88"}, {"b", "88"}});
        fdb5::Key index_key({{"c", "3"}, {"d", "4"}});
        fdb5::Key field_key({{"e", "5"}, {"f", "6"}});
        eckit::URI catalogue_uri;

        {
            fdb5::RadosCatalogueWriter writer{db_key, config};
            fdb5::Catalogue& cat = writer;
            cat.selectIndex(index_key);
            std::unique_ptr<fdb5::FieldLocation> loc(
                new fdb5::RadosFieldLocation(eckit::URI{"rados", "unused"}, eckit::Offset(0), eckit::Length(1)));
            static_cast<fdb5::CatalogueWriter&>(writer).archive(index_key, field_key, std::move(loc));
            cat.flush(0);

            // Default: everything enabled.
            EXPECT(cat.enabled(fdb5::ControlIdentifier::Retrieve));
            EXPECT(cat.enabled(fdb5::ControlIdentifier::List));
            EXPECT(cat.enabled(fdb5::ControlIdentifier::Archive));

            // hideContents disables List and Retrieve, leaves Archive.
            cat.hideContents();
            EXPECT_NOT(cat.enabled(fdb5::ControlIdentifier::Retrieve));
            EXPECT_NOT(cat.enabled(fdb5::ControlIdentifier::List));
            EXPECT(cat.enabled(fdb5::ControlIdentifier::Archive));
            catalogue_uri = cat.uri();
        }

        {
            fdb5::RadosCatalogueReader reader{db_key, config};
            fdb5::Catalogue& cat = reader;
            EXPECT(static_cast<fdb5::CatalogueReader&>(reader).open());
            // State survives process boundary.
            EXPECT_NOT(cat.enabled(fdb5::ControlIdentifier::Retrieve));
            EXPECT_NOT(cat.enabled(fdb5::ControlIdentifier::List));
            EXPECT(cat.enabled(fdb5::ControlIdentifier::Archive));
        }

        {
            auto reader = fdb5::CatalogueReaderFactory::instance().build(catalogue_uri, config);
            EXPECT_NOT(reader->enabled(fdb5::ControlIdentifier::Retrieve));
            EXPECT_NOT(reader->enabled(fdb5::ControlIdentifier::List));
            EXPECT(reader->enabled(fdb5::ControlIdentifier::Archive));
        }

        {
            fdb5::RadosCatalogueWriter writer{db_key, config};
            fdb5::Catalogue& cat = writer;
            fdb5::ControlIdentifiers ids = fdb5::ControlIdentifier::List | fdb5::ControlIdentifier::Retrieve;
            cat.control(fdb5::ControlAction::Enable, ids);
            EXPECT(cat.enabled(fdb5::ControlIdentifier::Retrieve));
            EXPECT(cat.enabled(fdb5::ControlIdentifier::List));
        }
    }

    SECTION("RadosCatalogueWriter supports concurrent writers on the same database") {

        std::string config_str{
            "spaces:\n"
            "- roots:\n"
            "  - path: " +
            catalogue_tests_tmp_root().asString() +
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
            "  catalogue:\n"
            "    pool: " +
            pool +
            "\n"
            "    root_namespace: " +
            test_id +
            "_root\n"
            "    namespace_prefix: " +
            test_id + "\n"};

        fdb5::Config config{YAMLConfiguration(config_str)};

        fdb5::Key db_key({{"a", "66"}, {"b", "66"}});
        fdb5::Key index_key({{"c", "3"}, {"d", "4"}});
        fdb5::Key first_field_key({{"e", "5"}});
        fdb5::Key second_field_key({{"g", "6"}});

        std::promise<void> start;
        const std::shared_future<void> ready = start.get_future().share();
        std::mutex error_mutex;
        std::exception_ptr error;

        //
        const auto archive = [&](const fdb5::Key& field_key, eckit::Offset offset) {
            try {
                ready.wait();
                fdb5::RadosCatalogueWriter writer{db_key, config};
                fdb5::Catalogue& catalogue = writer;
                catalogue.selectIndex(index_key);
                std::unique_ptr<fdb5::FieldLocation> location(
                    new fdb5::RadosFieldLocation(eckit::URI{"rados", "unused"}, offset, eckit::Length(1)));
                static_cast<fdb5::CatalogueWriter&>(writer).archive(index_key, field_key, std::move(location));
                catalogue.flush(0);
            }
            catch (...) {
                std::lock_guard<std::mutex> lock{error_mutex};
                if (!error) {
                    error = std::current_exception();
                }
            }
        };

        std::thread first{archive, std::cref(first_field_key), eckit::Offset(0)};
        std::thread second{archive, std::cref(second_field_key), eckit::Offset(1)};
        start.set_value();
        first.join();
        second.join();
        EXPECT(!error);

        fdb5::RadosCatalogueReader reader{db_key, config};
        fdb5::Catalogue& catalogue = reader;
        EXPECT(static_cast<fdb5::CatalogueReader&>(reader).open());
        EXPECT(catalogue.selectIndex(index_key));
        const auto e_axis = static_cast<fdb5::CatalogueReader&>(reader).axis("e");
        const auto g_axis = static_cast<fdb5::CatalogueReader&>(reader).axis("g");
        EXPECT(e_axis && e_axis->get().contains("5"));
        EXPECT(g_axis && g_axis->get().contains("6"));
    }

    SECTION("Rados placements are selected from matching space roots") {

        const std::string alpha_prefix = test_id + "alpha";
        const std::string beta_prefix = test_id + "beta";
        std::string config_str{
            "spaces:\n"
            "- regex: 11:11\n"
            "  roots:\n"
            "  - path: " +
            catalogue_tests_tmp_root().asString() +
            "\n"
            "    pool: " +
            pool +
            "\n"
            "    root_namespace: " +
            test_id +
            "_alpha_root\n"
            "    namespace_prefix: " +
            alpha_prefix +
            "\n"
            "- regex: 22:22\n"
            "  roots:\n"
            "  - path: " +
            catalogue_tests_tmp_root().asString() +
            "\n"
            "    pool: " +
            pool +
            "\n"
            "    root_namespace: " +
            test_id +
            "_beta_root\n"
            "    namespace_prefix: " +
            beta_prefix +
            "\n"
            "schema : " +
            schema_file().path() +
            "\n"
            "rados:\n"
            "  pool: " +
            pool +
            "\n"
            "  root_namespace: " +
            test_id +
            "_legacy_root\n"
            "  namespace_prefix: legacy\n"};

        fdb5::Config config{YAMLConfiguration(config_str)};
        fdb5::Key alpha_key({{"a", "11"}, {"b", "11"}});
        fdb5::Key beta_key({{"a", "22"}, {"b", "22"}});

        fdb5::RadosCatalogueWriter alpha{alpha_key, config};
        fdb5::RadosCatalogueWriter beta{beta_key, config};

        const std::string alpha_namespace = pool + "/" + alpha_prefix + "_" + alpha_key.valuesToString();
        const std::string beta_namespace = pool + "/" + beta_prefix + "_" + beta_key.valuesToString();
        EXPECT(alpha.uri().name() == alpha_namespace);
        EXPECT(beta.uri().name() == beta_namespace);
        EXPECT(fdb5::Engine::backend("rados").location(alpha_key, config).name() == alpha_namespace + "/catalogue_kv");
        EXPECT(fdb5::Engine::backend("rados").location(beta_key, config).name() == beta_namespace + "/catalogue_kv");

        const auto alpha_locations = fdb5::Engine::backend("rados").visitableLocations(alpha_key, config);
        const auto beta_locations = fdb5::Engine::backend("rados").visitableLocations(beta_key, config);
        EXPECT(alpha_locations.size() == 1);
        EXPECT(beta_locations.size() == 1);
        EXPECT(alpha_locations.front().name() == alpha_namespace + "/catalogue_kv");
        EXPECT(beta_locations.front().name() == beta_namespace + "/catalogue_kv");
    }

    SECTION("RadosCatalogue supports large serialised field locations") {

        std::string config_str{
            "spaces:\n"
            "- roots:\n"
            "  - path: " +
            catalogue_tests_tmp_root().asString() +
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
            "  pool: " +
            pool +
            "\n"
            "  root_namespace: " +
            test_id +
            "_root\n"
            "  namespace_prefix: " +
            test_id + "\n"};

        fdb5::Config config{YAMLConfiguration(config_str)};
        fdb5::Key db_key({{"a", "large"}, {"b", "large"}});
        fdb5::Key index_key({{"c", "large"}, {"d", "large"}});
        fdb5::Key field_key({{"e", "5"}, {"f", "6"}});

        auto location = std::make_unique<fdb5::RadosFieldLocation>(eckit::URI{"rados", std::string(600, 'x')},
                                                                   eckit::Offset(0), eckit::Length(1));

        {
            fdb5::RadosCatalogueWriter writer{db_key, config};
            fdb5::Catalogue& catalogue = writer;
            EXPECT(catalogue.selectIndex(index_key));
            static_cast<fdb5::CatalogueWriter&>(writer).archive(index_key, field_key, std::move(location));
            catalogue.flush(0);
        }

        {
            fdb5::RadosCatalogueReader reader{db_key, config};
            fdb5::Catalogue& catalogue = reader;
            EXPECT(catalogue.selectIndex(index_key));

            fdb5::Field field;
            EXPECT(static_cast<fdb5::CatalogueReader&>(reader).retrieve(field_key, field));
            EXPECT(field.location().uri().name() == std::string(600, 'x'));
        }
    }

    SECTION("Via FDB API with a Rados catalogue and store") {

#ifdef eckit_HAVE_RADOS_TESTS_MANAGE_POOLS
        eckit::RadosPool{pool}.ensureDestroyed();
        eckit::RadosPool{pool}.ensureCreated();
#else
        ensureCleanNamespaces(pool, test_id);
#endif

        // FDB configuration

        std::string config_str{
            "spaces:\n"
            "- roots:\n"
            "  - path: " +
            catalogue_tests_tmp_root().asString() +
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
            "engine: rados\n"
            "store: rados\n"
            "rados:\n"};

        config_str += "  pool: " + pool +
                      "\n"
                      "  root_namespace: " +
                      test_id +
                      "_root\n"
                      "  namespace_prefix: " +
                      test_id + "\n";

        fdb5::Config config{YAMLConfiguration(config_str)};

        // request

        fdb5::Key request_key({{"a", "11"}, {"b", "22"}, {"c", "3"}, {"d", "4"}, {"e", "5"}, {"f", "6"}});
        fdb5::Key db_key({{"a", "11"}, {"b", "22"}});
        fdb5::Key index_key({{"a", "11"}, {"b", "22"}, {"c", "3"}, {"d", "4"}});

        fdb5::FDBToolRequest full_req{request_key.request("retrieve"), false, std::vector<std::string>{"a", "b"}};
        fdb5::FDBToolRequest index_req{index_key.request("retrieve"), false, std::vector<std::string>{"a", "b"}};
        fdb5::FDBToolRequest db_req{db_key.request("retrieve"), false, std::vector<std::string>{"a", "b"}};
        fdb5::FDBToolRequest all_req{metkit::mars::MarsRequest{}, true, std::vector<std::string>{}};

        // initialise FDB

        fdb5::FDB fdb(config);

        // check FDB is empty

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

        // archive data

        char data[] = "test";

        fdb.archive(request_key, data, sizeof(data));
        fdb.flush();

        // retrieve data

        metkit::mars::MarsRequest r = request_key.request("retrieve");
        std::unique_ptr<eckit::DataHandle> dh(fdb.retrieve(r));

        eckit::MemoryHandle mh;
        dh->copyTo(mh);
        EXPECT(mh.size() == eckit::Length(sizeof(data)));
        EXPECT(::memcmp(mh.data(), data, sizeof(data)) == 0);

        fdb5::FDB reopened(config);
        std::unique_ptr<eckit::DataHandle> reopened_handle(reopened.retrieve(r));

        eckit::MemoryHandle reopened_data;
        reopened_handle->copyTo(reopened_data);
        EXPECT(reopened_data.size() == eckit::Length(sizeof(data)));
        EXPECT(::memcmp(reopened_data.data(), data, sizeof(data)) == 0);

        // list all

        listObject = fdb.list(all_req);
        count = 0;
        while (listObject.next(info)) {
            count++;
        }
        EXPECT(count == 1);

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
            count++;
        }
        EXPECT(count == 1);

        // attempt to wipe with too specific request
        wipeObject = fdb.wipe(full_req, true);
        EXPECT(countWipeable(wipeObject) == 0);
        fdb.flush();

        // wipe index and store unit
        wipeObject = fdb.wipe(index_req, true);
        EXPECT(countWipeable(wipeObject) > 0);
        fdb.flush();

        // ensure field does not exist
        listObject = fdb.list(full_req);
        count = 0;
        while (listObject.next(info)) {
            count++;
        }
        EXPECT(count == 0);

        // re-archive data

        // FDB caches open DBs. Once a full DB is wiped, a fresh FDB instance is needed
        // to re-create the top-level catalogue KV.
        fdb5::FDB fdb2(config);

        fdb2.archive(request_key, data, sizeof(data));
        fdb2.flush();

        listObject = fdb2.list(full_req);
        count = 0;
        while (listObject.next(info)) {
            count++;
        }
        EXPECT(count == 1);

        // Wipe remains enabled after hideContents disables only List and Retrieve.
        fdb2.control(db_req, fdb5::ControlAction::Disable,
                     fdb5::ControlIdentifier::List | fdb5::ControlIdentifier::Retrieve);
        wipeObject = fdb2.wipe(db_req, true);
        EXPECT(countWipeable(wipeObject) > 0);
        fdb2.flush();

        fdb5::RadosCatalogueReader hidden_reader{db_key, config};
        EXPECT_NOT(static_cast<fdb5::CatalogueReader&>(hidden_reader).open());

        // ensure field does not exist
        listObject = fdb2.list(full_req);
        count = 0;
        while (listObject.next(info)) {
            count++;
        }
        EXPECT(count == 0);

        // Wipe an already-wiped DB. The store-side namespace destroy path must be idempotent so
        // recovering from a partial wipe does not raise.
        EXPECT_NO_THROW(fdb2.wipe(db_req, true));
        fdb2.flush();
    }

    // teardown rados

#ifdef eckit_HAVE_RADOS_TESTS_MANAGE_POOLS
    eckit::RadosPool{pool}.ensureDestroyed();
#else
    ensureCleanNamespaces(pool, test_id);
#endif
}

}  // namespace fdb::test

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char** argv) {
    int ret = -1;
    try {
        ret = eckit::testing::run_tests(argc, argv);
    }
    catch (...) {
        eckit::Log::error() << "FDB RADOS catalogue tests terminated with an exception" << std::endl;
    }

    cleanupRados();
    return ret;
}
