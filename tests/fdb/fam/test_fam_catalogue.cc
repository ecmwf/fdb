/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the Horizon Europe programme funded project OpenCUBE
 * (Grant agreement: 101092984) horizon-opencube.eu
 */

/// @file   test_fam_catalogue.cc
/// @author Metin Cakircali
/// @date   Jun 2024

#include "test_fam_common.h"

#include <cstring>
#include <ctime>
#include <memory>
#include <string>
#include <vector>

#include "eckit/config/YAMLConfiguration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/io/Length.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/io/fam/FamRegionName.h"
#include "eckit/testing/Test.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Engine.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/Key.h"
#include "fdb5/fam/FamCatalogue.h"
#include "fdb5/fam/FamCatalogueReader.h"
#include "fdb5/fam/FamCatalogueWriter.h"
#include "fdb5/fam/FamCommon.h"
#include "fdb5/fam/FamStore.h"

using namespace eckit::testing;

namespace fdb::test {

//----------------------------------------------------------------------------------------------------------------------

namespace {

constexpr eckit::fam::size_t test_region_size = 1024 * 1024;  // 1 MB
constexpr eckit::fam::perm_t test_region_perm = 0640;
const auto test_fdb_fam_region = eckit::FamPath("test_fdb_catalogue");
const auto test_fdb_fam_uri = "fam://" + fam::test_fdb_fam_endpoint + "/" + test_fdb_fam_region.asString();

const std::string test_schema =
    "[ fam1a, fam1b, fam1c\n"
    "    [ fam2a, fam2b, fam2c\n"
    "       [ fam3a, fam3b, fam3c ]]]\n";

const std::string test_config =
    "---\n"
    "type: local\n"
    "engine: fam\n"
    "schema: ./schema\n"
    "store: fam\n"
    "catalogue: fam\n"
    "spaces:\n"
    "- handler: Default\n"
    "  roots:\n"
    "  - path: ./root\n"
    "fam_roots:\n"
    "- uri: " +
    test_fdb_fam_uri +
    "\n"
    "  writable: true\n"
    "  visit: true\n";

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

CASE("FamCatalogue: static naming helpers") {

    const auto db_key = fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}};
    const auto idx_key =
        fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}, {"fam2a", "d"}, {"fam2b", "e"}, {"fam2c", "f"}};

    const auto cat_name = fdb5::FamCatalogue::catalogueName(db_key);
    const auto idx_name = fdb5::FamCatalogue::indexName(idx_key);

    // Names must start with the expected prefix
    EXPECT(cat_name.find("cat-") == 0);
    EXPECT(idx_name.find("idx-") == 0);

    // Different keys produce different names
    EXPECT(cat_name != idx_name);

    // Same key produces the same name (deterministic)
    EXPECT_EQUAL(fdb5::FamCatalogue::catalogueName(db_key), cat_name);
    EXPECT_EQUAL(fdb5::FamCatalogue::indexName(idx_key), idx_name);
}

CASE("FamCatalogueWriter/Reader: direct OpenFAM metadata roundtrip") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto db_key = fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}};

    const auto idx_key =
        fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}, {"fam2a", "d"}, {"fam2b", "e"}, {"fam2c", "f"}};

    const auto datum_key = fdb5::Key({{"fam1a", "a"},
                                      {"fam1b", "b"},
                                      {"fam1c", "c"},
                                      {"fam2a", "d"},
                                      {"fam2b", "e"},
                                      {"fam2c", "f"},
                                      {"fam3a", "g"},
                                      {"fam3b", "h"},
                                      {"fam3c", "i"}});

    const char* data = "Testing fam: ARCHIVE DATA!";
    const auto data_length = std::char_traits<char>::length(data);

    // Archive data into the store first
    fdb5::FamStore fam_store(db_key, config);
    auto& store = static_cast<fdb5::Store&>(fam_store);
    auto location = store.archive(datum_key, data, eckit::Length(data_length));
    EXPECT(location);

    // Write metadata into the catalogue
    fdb5::FamCatalogueWriter writer(db_key, config);
    fdb5::Catalogue& writer_cat = writer;
    fdb5::CatalogueWriter& writer_iface = writer;

    EXPECT_EQUAL(writer_cat.type(), std::string("fam"));
    EXPECT_EQUAL(writer_cat.uri().scheme(), std::string("fam"));
    EXPECT(writer_cat.exists());

    EXPECT(writer_iface.createIndex(idx_key, datum_key.size()));
    writer_iface.archive(idx_key, datum_key, location->make_shared());
    writer_iface.flush(1);

    // Read metadata back via the reader
    const eckit::URI cat_uri = writer.uri();
    fdb5::FamCatalogueReader reader(cat_uri, config);
    fdb5::Catalogue& reader_cat = reader;

    EXPECT(reader.open());
    EXPECT_EQUAL(reader_cat.type(), std::string("fam"));
    EXPECT_EQUAL(reader_cat.uri().scheme(), std::string("fam"));
    EXPECT(reader_cat.exists());
    EXPECT(reader.selectIndex(idx_key));

    fdb5::Field field;
    EXPECT(reader.retrieve(datum_key, field));

    {
        std::unique_ptr<eckit::DataHandle> handle(field.dataHandle());
        eckit::MemoryHandle retrieved;
        handle->copyTo(retrieved);
        EXPECT_EQUAL(retrieved.size(), eckit::Length(data_length));
        EXPECT_EQUAL(::memcmp(retrieved.data(), data, data_length), 0);
    }

    // Verify FAM objects were created in the region
    const auto root = eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region);

    const auto cat_map_name = fdb5::FamCatalogue::catalogueName(db_key) + fdb5::FamCommon::table_suffix;
    const auto idx_map_name = fdb5::FamCatalogue::indexName(idx_key) + fdb5::FamCommon::table_suffix;
    const auto reg_map_name = std::string(fdb5::FamCommon::registry_keyword) + fdb5::FamCommon::table_suffix;

    EXPECT(root.object(reg_map_name).exists());
    EXPECT(root.object(cat_map_name).exists());
    EXPECT(root.object(idx_map_name).exists());

    // selectIndex for a non-existent key returns false
    const auto missing_idx =
        fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}, {"fam2a", "x"}, {"fam2b", "y"}, {"fam2c", "z"}};
    EXPECT(!reader.selectIndex(missing_idx));

    // Retrieve with the wrong datum key returns false
    const auto wrong_datum = fdb5::Key({{"fam1a", "a"},
                                        {"fam1b", "b"},
                                        {"fam1c", "c"},
                                        {"fam2a", "d"},
                                        {"fam2b", "e"},
                                        {"fam2c", "f"},
                                        {"fam3a", "x"},
                                        {"fam3b", "y"},
                                        {"fam3c", "z"}});
    reader.selectIndex(idx_key);
    fdb5::Field no_field;
    EXPECT(!reader.retrieve(wrong_datum, no_field));
}

CASE("FamCatalogueReader: loads schema from persisted FAM copy") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto db_key = fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}};

    // Seed the catalogue and persist schema into FAM.
    fdb5::FamCatalogueWriter writer(db_key, config);

    // Corrupt the local schema file after persistence.
    fam::write("[ invalid schema", setup.schemaPath);

    // Reader must still open successfully by loading schema from FAM.
    fdb5::FamCatalogueReader reader(writer.uri(), config);
    EXPECT(reader.open());
}

CASE("FamCatalogueWriter: persisted FAM schema is authoritative") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto db_key = fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}};

    // First writer persists schema.
    fdb5::FamCatalogueWriter writer1(db_key, config);

    // Corrupt local schema file. A second writer for the same DB must still
    // use the persisted schema and succeed.
    fam::write("[ invalid schema", setup.schemaPath);

    EXPECT_NO_THROW(fdb5::FamCatalogueWriter writer2(db_key, config));
}

CASE("FamCatalogueWriter: multiple indexes in one catalogue") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto db_key = fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}};

    const auto idx_key1 =
        fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}, {"fam2a", "d"}, {"fam2b", "e"}, {"fam2c", "f"}};
    const auto idx_key2 =
        fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}, {"fam2a", "x"}, {"fam2b", "y"}, {"fam2c", "z"}};

    const auto datum_key1 = fdb5::Key({{"fam1a", "a"},
                                       {"fam1b", "b"},
                                       {"fam1c", "c"},
                                       {"fam2a", "d"},
                                       {"fam2b", "e"},
                                       {"fam2c", "f"},
                                       {"fam3a", "g"},
                                       {"fam3b", "h"},
                                       {"fam3c", "i"}});

    const auto datum_key2 = fdb5::Key({{"fam1a", "a"},
                                       {"fam1b", "b"},
                                       {"fam1c", "c"},
                                       {"fam2a", "x"},
                                       {"fam2b", "y"},
                                       {"fam2c", "z"},
                                       {"fam3a", "p"},
                                       {"fam3b", "q"},
                                       {"fam3c", "r"}});

    const char* data1 = "Index-1 field data";
    const auto data1_length = std::char_traits<char>::length(data1);
    const char* data2 = "Index-2 field data";
    const auto data2_length = std::char_traits<char>::length(data2);

    fdb5::FamStore fam_store(db_key, config);
    auto& store = static_cast<fdb5::Store&>(fam_store);
    auto loc1 = store.archive(datum_key1, data1, eckit::Length(data1_length));
    auto loc2 = store.archive(datum_key2, data2, eckit::Length(data2_length));

    // Write two indexes through the CatalogueWriter interface
    fdb5::FamCatalogueWriter writer(db_key, config);
    fdb5::CatalogueWriter& writer_iface = writer;
    writer_iface.createIndex(idx_key1, datum_key1.size());
    writer_iface.archive(idx_key1, datum_key1, loc1->make_shared());
    writer_iface.createIndex(idx_key2, datum_key2.size());
    writer_iface.archive(idx_key2, datum_key2, loc2->make_shared());
    writer_iface.flush(2);

    // Read back with a fresh reader
    const eckit::URI cat_uri = writer.uri();
    fdb5::FamCatalogueReader reader(cat_uri, config);
    EXPECT(reader.open());

    // Both indexes must be selectable
    EXPECT(reader.selectIndex(idx_key1));
    fdb5::Field field1;
    EXPECT(reader.retrieve(datum_key1, field1));
    {
        std::unique_ptr<eckit::DataHandle> handle(field1.dataHandle());
        eckit::MemoryHandle retrieved;
        handle->copyTo(retrieved);
        EXPECT_EQUAL(retrieved.size(), eckit::Length(data1_length));
        EXPECT_EQUAL(::memcmp(retrieved.data(), data1, data1_length), 0);
    }

    EXPECT(reader.selectIndex(idx_key2));
    fdb5::Field field2;
    EXPECT(reader.retrieve(datum_key2, field2));
    {
        std::unique_ptr<eckit::DataHandle> handle(field2.dataHandle());
        eckit::MemoryHandle retrieved;
        handle->copyTo(retrieved);
        EXPECT_EQUAL(retrieved.size(), eckit::Length(data2_length));
        EXPECT_EQUAL(::memcmp(retrieved.data(), data2, data2_length), 0);
    }

    // indexes() should return both (access through Catalogue base class)
    const fdb5::Catalogue& reader_cat = reader;
    const auto idxs = reader_cat.indexes(false);
    EXPECT_EQUAL(idxs.size(), 2U);
}

CASE("FamEngine: canHandle and visitableLocations") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    // Access through the Engine base class (canHandle/visitableLocations are public there)
    fdb5::Engine& engine = fdb5::Engine::backend("fam");
    const eckit::URI root_uri(test_fdb_fam_uri);
    EXPECT(engine.canHandle(root_uri, config));

    // A non-fam URI should not be handled
    const eckit::URI file_uri("file:///tmp/some/path");
    EXPECT(!engine.canHandle(file_uri, config));

    // Write a catalogue so visitableLocations has something to find
    const auto db_key = fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}};
    { fdb5::FamCatalogueWriter writer(db_key, config); }

    const auto locations = engine.visitableLocations(db_key, config);
    EXPECT(!locations.empty());
    EXPECT_EQUAL(locations[0].scheme(), std::string("fam"));
}

CASE("FamCatalogue: full FDB archive, list, retrieve pipeline") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    TEST_LOG_DEBUG("SETUP FDB FAM");

    const fam::FamSetup setup(test_schema, test_config);

    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    fdb5::FDB fdb(config);

    //------------------------------------------------------------------------------------------------------------------

    const char* data = "Testing fam: ARCHIVE DATA!";
    const auto data_length = std::char_traits<char>::length(data);

    {
        TEST_LOG_INFO("ARCHIVE #1");

        auto key = fdb5::Key({{"fam1a", "val1a"},
                              {"fam1b", "val1b"},
                              {"fam1c", "val1c"},
                              {"fam2a", "val2a"},
                              {"fam2b", "val2b"},
                              {"fam2c", "val2c"},
                              {"fam3a", "val3a"},
                              {"fam3b", "val3b"},
                              {"fam3c", "val3c"}});

        const auto request = key.request("retrieve");

        {
            TEST_LOG_INFO("LIST THAT DB IS EMPTY");
            auto list = fdb.list(request);
            auto count = fam::count_list(list);
            EXPECT_EQUAL(count, 0);
        }

        fdb.archive(key, data, data_length);
        fdb.flush();

        {
            TEST_LOG_INFO("LIST FIRST ARCHIVED DATA");
            auto list = fdb.list(request);
            std::ostringstream out;
            auto count = fam::count_list(list, out);
            TEST_LOG_INFO("LIST OUTPUT:\n" << out.str());
            EXPECT(out.str().find("FamFieldLocation") != std::string::npos);
            EXPECT_EQUAL(count, 1);
        }
    }

    //------------------------------------------------------------------------------------------------------------------

    {
        TEST_LOG_INFO("ARCHIVE #2 — different datum key in same index");

        auto key = fdb5::Key({{"fam1a", "val1a"},
                              {"fam1b", "val1b"},
                              {"fam1c", "val1c"},
                              {"fam2a", "val2a"},
                              {"fam2b", "val2b"},
                              {"fam2c", "val2c"},
                              {"fam3a", "val3a"},
                              {"fam3b", "val3b"},
                              {"fam3c", "val33c"}});

        fdb.archive(key, data, data_length);
        fdb.flush();

        TEST_LOG_INFO("LIST SECOND ARCHIVED DATA");
        const fdb5::FDBToolRequest request(key.request("retrieve"), false,
                                           std::vector<std::string>{"fam1a", "fam1b", "fam1c"});

        auto list = fdb.list(request);
        std::ostringstream out;
        auto count = fam::count_list(list, out);
        TEST_LOG_INFO("LIST OUTPUT:\n" << out.str());
        EXPECT(out.str().find("FamFieldLocation") != std::string::npos);
        EXPECT_EQUAL(count, 1);
    }

    //------------------------------------------------------------------------------------------------------------------

    {
        TEST_LOG_INFO("LIST BOTH ARCHIVED DATA");
        auto key = fdb5::Key({
            {"fam1a", "val1a"},
            {"fam1b", "val1b"},
            {"fam1c", "val1c"},
            {"fam2a", "val2a"},
            {"fam2b", "val2b"},
            {"fam2c", "val2c"},
        });
        const auto request = key.request("retrieve");
        auto list = fdb.list(request);
        std::ostringstream out;
        auto count = fam::count_list(list, out);
        TEST_LOG_INFO("LIST OUTPUT:\n" << out.str());
        EXPECT_EQUAL(count, 2);
    }

    //------------------------------------------------------------------------------------------------------------------

    {
        TEST_LOG_INFO("RETRIEVE");

        auto key = fdb5::Key({{"fam1a", "val1a"},
                              {"fam1b", "val1b"},
                              {"fam1c", "val1c"},
                              {"fam2a", "val2a"},
                              {"fam2b", "val2b"},
                              {"fam2c", "val2c"},
                              {"fam3a", "val3a"},
                              {"fam3b", "val3b"},
                              {"fam3c", "val3c"}});

        const auto request = key.request("retrieve");

        std::unique_ptr<eckit::DataHandle> handle(fdb.retrieve(request));

        eckit::MemoryHandle retrieved;
        handle->copyTo(retrieved);
        EXPECT_EQUAL(retrieved.size(), eckit::Length(data_length));
        EXPECT_EQUAL(::memcmp(retrieved.data(), data, data_length), 0);
    }

    /// TODO(metin): Add a test case for retrieving with a partial key that matches multiple entries.
    /// TODO(metin): Add REMOVE test cases.

    TEST_LOG_INFO("FINISHED!");
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb::test

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
