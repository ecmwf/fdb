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
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "eckit/config/YAMLConfiguration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/io/Length.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/io/fam/FamRegionName.h"
#include "eckit/serialisation/HandleStream.h"
#include "eckit/testing/Test.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Engine.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/IndexLocation.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/WipeState.h"
#include "fdb5/fam/FamCatalogue.h"
#include "fdb5/fam/FamCatalogueReader.h"
#include "fdb5/fam/FamCatalogueWriter.h"
#include "fdb5/fam/FamCommon.h"
#include "fdb5/fam/FamEngine.h"
#include "fdb5/fam/FamFieldLocation.h"
#include "fdb5/fam/FamIndex.h"
#include "fdb5/fam/FamIndexLocation.h"
#include "fdb5/fam/FamStore.h"

using namespace eckit::testing;

namespace fdb::test {

//----------------------------------------------------------------------------------------------------------------------

namespace {

constexpr eckit::fam::size_t test_region_size = 1024 * 1024;  // 1 MB
constexpr eckit::fam::perm_t test_region_perm = 0640;
const auto test_fdb_fam_region = eckit::FamPath("test_fdb_catalogue");
const auto test_fdb_fam_uri = "fam://" + fam::test_fdb_fam_endpoint + "/" + test_fdb_fam_region.asString();

const std::string test_config = fam::make_test_config(test_fdb_fam_uri);

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

CASE("FamCatalogue: static naming helpers") {

    const auto db_key = fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}};
    const auto idx_key =
        fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}, {"fam2a", "d"}, {"fam2b", "e"}, {"fam2c", "f"}};

    const auto cat_name = fdb5::FamCatalogue::catalogueName(db_key);
    const auto idx_name = fdb5::FamCatalogue::indexName(cat_name, idx_key);

    // Names must start with the expected prefix
    EXPECT(cat_name.find(fdb5::FamCommon::catalogue_prefix) == 0);
    EXPECT(idx_name.find(fdb5::FamCommon::index_prefix) == 0);

    // Different keys produce different names
    EXPECT(cat_name != idx_name);

    // Same key produces the same name (deterministic)
    EXPECT_EQUAL(fdb5::FamCatalogue::catalogueName(db_key), cat_name);
    EXPECT_EQUAL(fdb5::FamCatalogue::indexName(cat_name, idx_key), idx_name);
}

CASE("FamCatalogueWriter/Reader: direct OpenFAM metadata roundtrip") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
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
    fdb5::Store& store = fam_store;
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

    // dump — simple mode: header + index key, no axes or fields
    {
        std::ostringstream out;
        reader_cat.dump(out, true);
        const auto dump_str = out.str();
        EXPECT(dump_str.find("FamCatalogue") != std::string::npos);
        EXPECT(dump_str.find("{fam1a=a,fam1b=b,fam1c=c}") != std::string::npos);
        EXPECT(dump_str.find("Key: {fam1a=a,fam1b=b,fam1c=c,fam2a=d,fam2b=e,fam2c=f}") != std::string::npos);
        // Simple mode: no axes or field contents
        EXPECT(dump_str.find("Axes:") == std::string::npos);
        EXPECT(dump_str.find("Contents of index:") == std::string::npos);
    }

    // dump — verbose mode: header + index key + axes + field entries
    {
        std::ostringstream out;
        reader_cat.dump(out, false);
        const auto dump_str = out.str();
        EXPECT(dump_str.find("FamCatalogue") != std::string::npos);
        EXPECT(dump_str.find("Key: {fam1a=a,fam1b=b,fam1c=c,fam2a=d,fam2b=e,fam2c=f}") != std::string::npos);
        // Verbose: axes are printed
        EXPECT(dump_str.find("Axes:") != std::string::npos);
        EXPECT(dump_str.find("fam3a") != std::string::npos);
        EXPECT(dump_str.find("fam3c") != std::string::npos);
        // Verbose: field contents are printed
        EXPECT(dump_str.find("Contents of index:") != std::string::npos);
        EXPECT(dump_str.find("Fingerprint: a-b-c-d-e-f-g-h-i") != std::string::npos);
        EXPECT(dump_str.find("FamFieldLocation") != std::string::npos);
    }

    // Verify FAM objects were created in the region
    const auto root = eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region);

    const auto cat_name = fdb5::FamCatalogue::catalogueName(db_key);
    const auto cat_map_name = cat_name + fdb5::FamCommon::table_suffix;
    const auto idx_map_name = fdb5::FamCatalogue::indexName(cat_name, idx_key) + fdb5::FamCommon::table_suffix;
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

    const fam::FamSetup setup(fam::test_schema, test_config);
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

    const fam::FamSetup setup(fam::test_schema, test_config);
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

    const fam::FamSetup setup(fam::test_schema, test_config);
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
    fdb5::Store& store = fam_store;
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

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    // Access through the Engine base class (canHandle/visitableLocations are public there)
    auto& engine = fdb5::Engine::backend("fam");
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

    const fam::FamSetup setup(fam::test_schema, test_config);

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
// FamCatalogue: wipe and stub coverage
//----------------------------------------------------------------------------------------------------------------------

CASE("FamCatalogue: wipe methods work correctly") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto db_key = fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}};

    fdb5::FamCatalogueWriter writer(db_key, config);
    fdb5::Catalogue& cat = writer;

    // doUnsafeFullWipe clears catalogue + registry
    EXPECT(cat.doUnsafeFullWipe());

    // doWipeEmptyDatabase is a no-op when cleanupEmptyDatabase_ is false
    EXPECT_NO_THROW(cat.doWipeEmptyDatabase());

    // doWipeUnknowns returns true (best-effort removal)
    std::set<eckit::URI> unknowns{eckit::URI("fam://host:1234/region/object")};
    EXPECT(cat.doWipeUnknowns(unknowns));

    // doWipeURIs returns true
    fdb5::CatalogueWipeState wipe_state(db_key);
    EXPECT(cat.doWipeURIs(wipe_state));

    // markIndexForWipe returns true for index belonging to same region
    auto idx_key =
        fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}, {"fam2a", "d"}, {"fam2b", "e"}, {"fam2c", "f"}};
    fdb5::CatalogueWriter& writer_iface = writer;
    writer_iface.createIndex(idx_key, 3);
    writer_iface.selectIndex(idx_key);
    auto index = writer_iface.currentIndex();
    EXPECT(cat.markIndexForWipe(index, true, wipe_state));
}

CASE("FamCatalogue: wipeInit returns db key") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto db_key = fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}};

    fdb5::FamCatalogueWriter writer(db_key, config);
    fdb5::Catalogue& cat = writer;

    auto wipe = cat.wipeInit();
    // wipeInit should return a CatalogueWipeState (constructible from db_key)
    EXPECT_NO_THROW(wipe.sanityCheck());
}

CASE("FamCatalogue: finaliseWipeState with non-empty safeURIs marks safe") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto db_key = fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}};

    fdb5::FamCatalogueWriter writer(db_key, config);
    fdb5::Catalogue& cat = writer;

    // With safeURIs present, finalise marks cat URI as safe
    fdb5::CatalogueWipeState wipe_state(db_key);
    wipe_state.markAsSafe({eckit::URI("fam://host:1234/safe_object")});
    cat.finaliseWipeState(wipe_state);
    EXPECT(!wipe_state.safeURIs().empty());

    // With empty safeURIs (full wipe), marks cat for deletion
    fdb5::CatalogueWipeState full_wipe(db_key);
    cat.finaliseWipeState(full_wipe);
    EXPECT(full_wipe.countURIsToDelete() > 0);
}

CASE("FamCatalogue: NOTIMP methods, enabled, and control") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto db_key = fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}};

    fdb5::FamCatalogueWriter writer(db_key, config);
    fdb5::FamStore fam_store(db_key, config);
    fdb5::Catalogue& cat = writer;

    // NOTIMP methods
    EXPECT_THROWS(cat.checkUID());
    EXPECT_THROWS(cat.statsReportVisitor());
    EXPECT_THROWS(cat.purgeVisitor(fam_store));

    metkit::mars::MarsRequest request("retrieve");
    eckit::URI dest("fam://host:1234/dest");
    eckit::Queue<fdb5::MoveElement> queue(1);
    EXPECT_THROWS(cat.moveVisitor(fam_store, request, dest, queue));

    // enabled always returns true
    EXPECT(cat.enabled(fdb5::ControlIdentifier::List));
    EXPECT(cat.enabled(fdb5::ControlIdentifier::Archive));
    EXPECT(cat.enabled(fdb5::ControlIdentifier::Retrieve));

    // control is a no-op
    fdb5::ControlIdentifiers ids;
    EXPECT_NO_THROW(cat.control(fdb5::ControlAction::Disable, ids));

    // dump — empty catalogue, simple mode
    {
        std::ostringstream out;
        const std::string yaml_str("---\n");
        eckit::YAMLConfiguration empty_conf(yaml_str);
        EXPECT_NO_THROW(cat.dump(out, true, empty_conf));
        const auto dump_str = out.str();
        EXPECT(dump_str.find("FamCatalogue") != std::string::npos);
        EXPECT(dump_str.find("{fam1a=a,fam1b=b,fam1c=c}") != std::string::npos);
    }
}

//----------------------------------------------------------------------------------------------------------------------
// FamCatalogueWriter: NOTIMP methods and print
//----------------------------------------------------------------------------------------------------------------------

CASE("FamCatalogueWriter: NOTIMP methods and print") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto db_key = fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}};

    fdb5::FamCatalogueWriter writer(db_key, config);
    fdb5::CatalogueWriter& iface = writer;

    EXPECT_THROWS(iface.index(fdb5::Key(), eckit::URI(), eckit::Offset(0), eckit::Length(0)));
    EXPECT_THROWS(iface.reconsolidate());
    EXPECT_THROWS(iface.overlayDB(writer, {}, false));

    std::ostringstream out;
    out << writer;
    EXPECT(out.str().find("FamCatalogueWriter") != std::string::npos);
}

//----------------------------------------------------------------------------------------------------------------------
// FamCatalogueReader: NOTIMP, print, and inline methods
//----------------------------------------------------------------------------------------------------------------------

CASE("FamCatalogueReader: NOTIMP, print, and inline methods") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto db_key = fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}};

    { fdb5::FamCatalogueWriter writer(db_key, config); }

    fdb5::FamCatalogueReader reader(db_key, config);
    EXPECT(reader.open());

    EXPECT_THROWS(reader.stats());

    std::ostringstream out;
    out << reader;
    EXPECT(out.str().find("FamCatalogueReader") != std::string::npos);

    EXPECT_NO_THROW(reader.flush(0));
    EXPECT_NO_THROW(reader.clean());
    EXPECT_NO_THROW(reader.close());
}

//----------------------------------------------------------------------------------------------------------------------
// FamEngine: NOTIMP, canHandle, and visitableLocations
//----------------------------------------------------------------------------------------------------------------------

CASE("FamEngine: NOTIMP methods and name") {

    auto& engine = fdb5::Engine::backend("fam");
    EXPECT_EQUAL(engine.name(), std::string("fam"));
    EXPECT_THROWS(engine.location(fdb5::Key(), fdb5::Config()));
    EXPECT_THROWS(engine.dbType());
}

CASE("FamEngine: visitableLocations with MarsRequest") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto db_key = fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}};

    // Seed the catalogue
    { fdb5::FamCatalogueWriter writer(db_key, config); }

    auto& engine = fdb5::Engine::backend("fam");

    // Build a MarsRequest that matches
    metkit::mars::MarsRequest request("retrieve");
    request.setValue("fam1a", "a");
    request.setValue("fam1b", "b");
    request.setValue("fam1c", "c");

    const auto locations = engine.visitableLocations(request, config);
    EXPECT(!locations.empty());
    EXPECT_EQUAL(locations[0].scheme(), std::string("fam"));

    // Non-matching request should return empty
    metkit::mars::MarsRequest no_match("retrieve");
    no_match.setValue("fam1a", "zzz");
    const auto no_locs = engine.visitableLocations(no_match, config);
    EXPECT(no_locs.empty());
}

CASE("FamEngine: rootURI parses config") {

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    auto root = fdb5::FamEngine::rootURI(config);
    EXPECT_EQUAL(root.scheme(), std::string("fam"));

    // Missing fam_roots should throw
    const std::string bad_config = "---\ntype: local\n";
    fam::write(bad_config, setup.configPath);
    const auto bad = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};
    EXPECT_THROWS_AS(fdb5::FamEngine::rootURI(bad), eckit::BadValue);
}

//----------------------------------------------------------------------------------------------------------------------
// FamIndex: NOTIMP, stubs, and data URIs
//----------------------------------------------------------------------------------------------------------------------

CASE("FamIndex: NOTIMP and stub methods") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto db_key = fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}};
    const auto idx_key =
        fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}, {"fam2a", "d"}, {"fam2b", "e"}, {"fam2c", "f"}};

    eckit::FamRegionName root_name(fam::test_fdb_fam_endpoint, test_fdb_fam_region);

    // Allocate on heap — Index takes ownership via reference counting
    const auto cat_name = fdb5::FamCatalogue::catalogueName(db_key);
    fdb5::Index idx(new fdb5::FamIndex(idx_key, root_name, fdb5::FamCatalogue::indexName(cat_name, idx_key), false));

    // NOTIMP methods (flock/funlock are public on IndexBase)
    EXPECT_THROWS(idx.flock());
    EXPECT_THROWS(idx.funlock());
    EXPECT_THROWS(idx.reopen());

    // Stubs
    EXPECT(idx.dataURIs().empty());
    EXPECT(!idx.dirty());

    // open/close are no-ops
    EXPECT_NO_THROW(idx.open());
    EXPECT_NO_THROW(idx.close());

    // flush sorts axes (no-op when empty)
    EXPECT_NO_THROW(idx.flush());

    // print
    std::ostringstream out;
    out << idx;
    EXPECT(out.str().find("FamIndex") != std::string::npos);

    // location returns FamIndexLocation
    EXPECT_EQUAL(idx.location().uri().scheme(), std::string("fam"));
}

//----------------------------------------------------------------------------------------------------------------------
// FamFieldLocation: print, make_shared, and visit
//----------------------------------------------------------------------------------------------------------------------

CASE("FamFieldLocation: print and make_shared") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto store_key = fdb5::Key({{"fam1a", "v1a"}, {"fam1b", "v1b"}, {"fam1c", "v1c"}});

    // Archive to get a real FamFieldLocation
    fdb5::FamStore fam_store(store_key, config);
    fdb5::Store& store = fam_store;

    const char* data = "field-location-test";
    const auto data_length = std::char_traits<char>::length(data);

    auto key = fdb5::Key({{"fam1a", "v1a"},
                          {"fam1b", "v1b"},
                          {"fam1c", "v1c"},
                          {"fam2a", "v2a"},
                          {"fam2b", "v2b"},
                          {"fam2c", "v2c"},
                          {"fam3a", "v3a"},
                          {"fam3b", "v3b"},
                          {"fam3c", "v3c"}});

    auto floc = store.archive(key, data, data_length);
    EXPECT(floc);

    // print
    std::ostringstream out;
    out << *floc;
    EXPECT(out.str().find("FamFieldLocation") != std::string::npos);

    // make_shared
    auto shared = floc->make_shared();
    EXPECT(shared);
    EXPECT_EQUAL(shared->length(), eckit::Length(data_length));

    // URI-only constructor
    fdb5::FamFieldLocation loc1(floc->uri());
    EXPECT_EQUAL(loc1.uri().scheme(), std::string("fam"));
}

//----------------------------------------------------------------------------------------------------------------------
// FamIndexLocation: uri, clone, print, encode
//----------------------------------------------------------------------------------------------------------------------

CASE("FamIndexLocation: uri, clone, print, encode") {

    const eckit::URI test_uri("fam://" + fam::test_fdb_fam_endpoint + "/test_region/test_object");
    fdb5::FamIndexLocation loc(test_uri);

    EXPECT_EQUAL(loc.uri().scheme(), std::string("fam"));
    EXPECT_EQUAL(loc.uri(), test_uri);

    // clone
    auto cloned = std::unique_ptr<fdb5::IndexLocation>(loc.clone());
    EXPECT(cloned);
    EXPECT_EQUAL(cloned->uri(), test_uri);

    // print
    std::ostringstream out;
    out << loc;
    EXPECT(out.str().find("FamIndexLocation") != std::string::npos);
}

//----------------------------------------------------------------------------------------------------------------------
// FamCatalogue: edge cases
//----------------------------------------------------------------------------------------------------------------------

CASE("FamCatalogue: indexes returns empty when catalogue does not exist") {

    // Use a fresh region name that has no catalogue seeded
    const auto fresh_region = eckit::FamPath("test_fdb_catalogue_empty");
    eckit::FamRegionName(fam::test_fdb_fam_endpoint, fresh_region).create(test_region_size, test_region_perm, true);

    const std::string fresh_uri = "fam://" + fam::test_fdb_fam_endpoint + "/" + fresh_region.asString();

    const std::string alt_config =
        "---\ntype: local\nengine: fam\nschema: ./schema\nstore: fam\ncatalogue: fam\n"
        "spaces:\n- handler: Default\n  roots:\n  - path: ./root\n"
        "fam_roots:\n- uri: " +
        fresh_uri + "\n  writable: true\n  visit: true\n";

    const fam::FamSetup setup(fam::test_schema, alt_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto db_key = fdb5::Key{{"fam1a", "x"}, {"fam1b", "y"}, {"fam1c", "z"}};

    // Construct with Key to avoid looking up the catalogue (which doesn't exist yet)
    fdb5::FamCatalogueReader reader(db_key, config);
    fdb5::Catalogue& cat = reader;

    // indexes() should return empty when the catalogue FamMap doesn't exist
    auto idxs = cat.indexes(false);
    EXPECT(idxs.empty());
}

//----------------------------------------------------------------------------------------------------------------------
// FamCatalogueWriter: URI constructor and selectIndex cache hit
//----------------------------------------------------------------------------------------------------------------------

CASE("FamCatalogueWriter: URI-based construction") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto db_key = fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}};

    // First create with Key constructor to seed the catalogue
    eckit::URI cat_uri;
    {
        fdb5::FamCatalogueWriter writer(db_key, config);
        cat_uri = writer.uri();
    }

    // Now construct using the URI
    fdb5::FamCatalogueWriter writer(cat_uri, config);
    fdb5::Catalogue& cat = writer;
    EXPECT_EQUAL(cat.type(), std::string("fam"));
    EXPECT(cat.exists());
}

CASE("FamCatalogueWriter: selectIndex cache hit path") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
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

    fdb5::FamStore fam_store(db_key, config);
    fdb5::Store& store = fam_store;

    const char* data = "cache-hit-data";
    const auto data_length = std::char_traits<char>::length(data);
    auto loc1 = store.archive(datum_key1, data, eckit::Length(data_length));
    auto loc2 = store.archive(datum_key2, data, eckit::Length(data_length));

    fdb5::FamCatalogueWriter writer(db_key, config);
    fdb5::CatalogueWriter& writer_iface = writer;

    // First call creates idx_key1 in the cache
    writer_iface.archive(idx_key1, datum_key1, loc1->make_shared());
    // Second call creates idx_key2 in the cache (switches away from idx_key1)
    writer_iface.archive(idx_key2, datum_key2, loc2->make_shared());
    // Third call should hit the indexes_ cache for idx_key1 (not the fast path)
    writer_iface.archive(idx_key1, datum_key1, loc1->make_shared());
    writer_iface.flush(3);
}

//----------------------------------------------------------------------------------------------------------------------
// FamCatalogueReader: edge cases
//----------------------------------------------------------------------------------------------------------------------

CASE("FamCatalogueReader: retrieve returns false when no index selected") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto db_key = fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}};

    // Seed catalogue
    { fdb5::FamCatalogueWriter writer(db_key, config); }

    fdb5::FamCatalogueReader reader(db_key, config);
    EXPECT(reader.open());

    // Without calling selectIndex, current_ is null → retrieve returns false
    const auto datum_key = fdb5::Key({{"fam1a", "a"},
                                      {"fam1b", "b"},
                                      {"fam1c", "c"},
                                      {"fam2a", "d"},
                                      {"fam2b", "e"},
                                      {"fam2c", "f"},
                                      {"fam3a", "g"},
                                      {"fam3b", "h"},
                                      {"fam3c", "i"}});
    fdb5::Field field;
    EXPECT(!reader.retrieve(datum_key, field));
}

CASE("FamCatalogueReader: open returns false when catalogue does not exist") {

    const auto fresh_region = eckit::FamPath("test_fdb_reader_no_exist");
    eckit::FamRegionName(fam::test_fdb_fam_endpoint, fresh_region).create(test_region_size, test_region_perm, true);

    const std::string fresh_uri = "fam://" + fam::test_fdb_fam_endpoint + "/" + fresh_region.asString();
    const std::string alt_config =
        "---\ntype: local\nengine: fam\nschema: ./schema\nstore: fam\ncatalogue: fam\n"
        "spaces:\n- handler: Default\n  roots:\n  - path: ./root\n"
        "fam_roots:\n- uri: " +
        fresh_uri + "\n  writable: true\n  visit: true\n";

    const fam::FamSetup setup(fam::test_schema, alt_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto db_key = fdb5::Key{{"fam1a", "x"}, {"fam1b", "y"}, {"fam1c", "z"}};
    fdb5::FamCatalogueReader reader(db_key, config);

    // Catalogue doesn't exist → open returns false
    EXPECT(!reader.open());
}

//----------------------------------------------------------------------------------------------------------------------
// FamEngine: edge cases
//----------------------------------------------------------------------------------------------------------------------

CASE("FamEngine: visitableLocations with non-existent region") {

    // Use a region that doesn't exist at all
    const std::string no_region_uri = "fam://" + fam::test_fdb_fam_endpoint + "/nonexistent_region_for_test";
    const std::string alt_config =
        "---\ntype: local\nengine: fam\nschema: ./schema\nstore: fam\ncatalogue: fam\n"
        "spaces:\n- handler: Default\n  roots:\n  - path: ./root\n"
        "fam_roots:\n- uri: " +
        no_region_uri + "\n  writable: true\n  visit: true\n";

    const fam::FamSetup setup(fam::test_schema, alt_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    auto& engine = fdb5::Engine::backend("fam");

    // Key overload: region doesn't exist → returns empty
    const auto db_key = fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}};
    const auto locs_key = engine.visitableLocations(db_key, config);
    EXPECT(locs_key.empty());

    // MarsRequest overload: region doesn't exist → returns empty
    metkit::mars::MarsRequest request("retrieve");
    request.setValue("fam1a", "a");
    const auto locs_req = engine.visitableLocations(request, config);
    EXPECT(locs_req.empty());
}

CASE("FamEngine: visitableLocations with empty registry") {

    // Create a region with no catalogues registered
    const auto empty_region = eckit::FamPath("test_fdb_engine_empty_reg");
    eckit::FamRegionName(fam::test_fdb_fam_endpoint, empty_region).create(test_region_size, test_region_perm, true);

    const std::string empty_uri = "fam://" + fam::test_fdb_fam_endpoint + "/" + empty_region.asString();
    const std::string alt_config =
        "---\ntype: local\nengine: fam\nschema: ./schema\nstore: fam\ncatalogue: fam\n"
        "spaces:\n- handler: Default\n  roots:\n  - path: ./root\n"
        "fam_roots:\n- uri: " +
        empty_uri + "\n  writable: true\n  visit: true\n";

    const fam::FamSetup setup(fam::test_schema, alt_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    auto& engine = fdb5::Engine::backend("fam");

    // Key overload: region exists but no registry map → returns empty via outer catch or empty map
    const auto db_key = fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}};
    const auto locs_key = engine.visitableLocations(db_key, config);
    EXPECT(locs_key.empty());

    // MarsRequest overload: same
    metkit::mars::MarsRequest request("retrieve");
    request.setValue("fam1a", "a");
    const auto locs_req = engine.visitableLocations(request, config);
    EXPECT(locs_req.empty());
}

CASE("FamEngine: canHandle with invalid FAM URI") {

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    auto& engine = fdb5::Engine::backend("fam");

    // URI with fam scheme but pointing to a non-existent region → canHandle returns false
    const eckit::URI bad_fam("fam://" + fam::test_fdb_fam_endpoint + "/definitely_nonexistent_region");
    EXPECT(!engine.canHandle(bad_fam, config));
}

//----------------------------------------------------------------------------------------------------------------------
// FamIndex: dump, encode, visit, statistics
//----------------------------------------------------------------------------------------------------------------------

CASE("FamIndex: dump, encode, visit, statistics") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto idx_key =
        fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}, {"fam2a", "d"}, {"fam2b", "e"}, {"fam2c", "f"}};

    eckit::FamRegionName root_name(fam::test_fdb_fam_endpoint, test_fdb_fam_region);
    const auto cat_name = fdb5::FamCatalogue::catalogueName(idx_key);
    fdb5::Index idx(new fdb5::FamIndex(idx_key, root_name, fdb5::FamCatalogue::indexName(cat_name, idx_key), false));

    // dump — simple mode
    {
        std::ostringstream out;
        idx.dump(out, "  ", true, false);
        EXPECT(out.str().find("Key:") != std::string::npos);
    }

    // dump — full mode
    {
        std::ostringstream out;
        idx.dump(out, "  ", false, true);
        EXPECT(out.str().find("Key:") != std::string::npos);
    }

    // encode — NOTIMP
    eckit::MemoryHandle h(1024);
    h.openForWrite(0);
    eckit::HandleStream hs(h);
    EXPECT_THROWS(idx.encode(hs, 1));
    h.close();

    // visit — requires an IndexLocationVisitor
    struct TestVisitor : fdb5::IndexLocationVisitor {
        void operator()(const fdb5::IndexLocation&) override {}
    };
    TestVisitor visitor;
    EXPECT_THROWS(idx.visit(visitor));

    // statistics
    EXPECT_THROWS(idx.statistics());
}

//----------------------------------------------------------------------------------------------------------------------
// FamFieldLocation: visit
//----------------------------------------------------------------------------------------------------------------------

CASE("FamFieldLocation: visit dispatches to visitor") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto store_key = fdb5::Key({{"fam1a", "v1a"}, {"fam1b", "v1b"}, {"fam1c", "v1c"}});

    fdb5::FamStore fam_store(store_key, config);
    fdb5::Store& store = fam_store;

    const char* data = "visit-test-data";
    const auto data_length = std::char_traits<char>::length(data);

    auto key = fdb5::Key({{"fam1a", "v1a"},
                          {"fam1b", "v1b"},
                          {"fam1c", "v1c"},
                          {"fam2a", "v2a"},
                          {"fam2b", "v2b"},
                          {"fam2c", "v2c"},
                          {"fam3a", "v3a"},
                          {"fam3b", "v3b"},
                          {"fam3c", "v3c"}});

    auto floc = store.archive(key, data, data_length);
    EXPECT(floc);

    // Use FieldLocationPrinter which is a concrete FieldLocationVisitor
    std::ostringstream out;
    fdb5::FieldLocationPrinter printer(out);
    floc->visit(printer);
    EXPECT(!out.str().empty());
}

//----------------------------------------------------------------------------------------------------------------------
// FamCatalogue: maskIndexEntries, dataURIs, end-to-end wipe
//----------------------------------------------------------------------------------------------------------------------

CASE("FamCatalogue: maskIndexEntries removes index from catalogue") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto db_key = fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}};
    const auto idx_key =
        fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}, {"fam2a", "d"}, {"fam2b", "e"}, {"fam2c", "f"}};

    const char* data = "mask-test-data";
    const auto data_length = std::char_traits<char>::length(data);
    const auto datum_key = fdb5::Key({{"fam1a", "a"},
                                      {"fam1b", "b"},
                                      {"fam1c", "c"},
                                      {"fam2a", "d"},
                                      {"fam2b", "e"},
                                      {"fam2c", "f"},
                                      {"fam3a", "g"},
                                      {"fam3b", "h"},
                                      {"fam3c", "i"}});

    fdb5::FamStore fam_store(db_key, config);
    fdb5::Store& store = fam_store;
    auto loc = store.archive(datum_key, data, eckit::Length(data_length));

    fdb5::FamCatalogueWriter writer(db_key, config);
    fdb5::CatalogueWriter& writer_iface = writer;
    writer_iface.archive(idx_key, datum_key, loc->make_shared());
    writer_iface.flush(1);

    // Verify index exists
    fdb5::Catalogue& cat = writer;
    EXPECT_EQUAL(cat.indexes().size(), 1U);

    // Mask the index
    auto idxs = cat.indexes();
    std::set<fdb5::Index> to_mask(idxs.begin(), idxs.end());
    cat.maskIndexEntries(to_mask);

    // After masking, indexes() should return empty (entry removed from catalogue map)
    fdb5::FamCatalogueReader reader(writer.uri(), config);
    EXPECT(reader.open());
    fdb5::Catalogue& reader_cat = reader;
    EXPECT(reader_cat.indexes().empty());
}

CASE("FamIndex: dataURIs returns field locations after archive") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto db_key = fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}};
    const auto idx_key =
        fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}, {"fam2a", "d"}, {"fam2b", "e"}, {"fam2c", "f"}};

    const char* data1 = "data-uri-test-1";
    const auto data1_length = std::char_traits<char>::length(data1);
    const char* data2 = "data-uri-test-2";
    const auto data2_length = std::char_traits<char>::length(data2);

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
                                       {"fam2a", "d"},
                                       {"fam2b", "e"},
                                       {"fam2c", "f"},
                                       {"fam3a", "x"},
                                       {"fam3b", "y"},
                                       {"fam3c", "z"}});

    fdb5::FamStore fam_store(db_key, config);
    fdb5::Store& store = fam_store;
    auto loc1 = store.archive(datum_key1, data1, eckit::Length(data1_length));
    auto loc2 = store.archive(datum_key2, data2, eckit::Length(data2_length));

    fdb5::FamCatalogueWriter writer(db_key, config);
    fdb5::CatalogueWriter& writer_iface = writer;
    writer_iface.archive(idx_key, datum_key1, loc1->make_shared());
    writer_iface.archive(idx_key, datum_key2, loc2->make_shared());
    writer_iface.flush(2);

    // Retrieve the index and check dataURIs
    fdb5::Catalogue& cat = writer;
    auto idxs = cat.indexes();
    EXPECT_EQUAL(idxs.size(), 1U);

    auto data_uris = idxs[0].dataURIs();
    EXPECT_EQUAL(data_uris.size(), 2U);

    // All returned URIs should be fam:// scheme and point to actual data objects
    for (const auto& uri : data_uris) {
        EXPECT_EQUAL(uri.scheme(), std::string("fam"));
        EXPECT(fam_store.uriBelongs(uri));
        EXPECT(fam_store.uriExists(uri));
    }
}

CASE("FamCatalogue: end-to-end wipe removes catalogue and data") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
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

    const char* data = "wipe-e2e-data";
    const auto data_length = std::char_traits<char>::length(data);

    // 1. Archive data and metadata
    fdb5::FamStore fam_store(db_key, config);
    fdb5::Store& store = fam_store;
    auto loc = store.archive(datum_key, data, eckit::Length(data_length));
    EXPECT(loc);
    EXPECT(fam_store.uriExists(loc->uri()));

    fdb5::FamCatalogueWriter writer(db_key, config);
    fdb5::CatalogueWriter& writer_iface = writer;
    writer_iface.archive(idx_key, datum_key, loc->make_shared());
    writer_iface.flush(1);

    fdb5::Catalogue& cat = writer;

    // Verify everything exists before wipe
    EXPECT(cat.exists());
    EXPECT_EQUAL(cat.indexes().size(), 1U);

    // 2. Verify the engine can discover this DB
    auto& engine = fdb5::Engine::backend("fam");
    EXPECT(!engine.visitableLocations(db_key, config).empty());

    // 3. Perform doUnsafeFullWipe (clears all maps + deregisters from registry)
    EXPECT(cat.doUnsafeFullWipe());

    // 4. Verify catalogue data is gone — engine should no longer discover it
    EXPECT(engine.visitableLocations(db_key, config).empty());

    // 5. Data objects in the store are not affected by catalogue wipe (separate concern)
    //    Store wipe is tested in test_fam_store.cc
}

//----------------------------------------------------------------------------------------------------------------------

CASE("FamIndex: concurrent writers flush axes without data loss") {
    // Stress test: multiple processes writing disjoint fields to the SAME FamIndex,
    // flushing multiple times mid-stream, then verifying every axes value survives.
    // This simulates MPI tasks that share a FAM region and write concurrently
    // to the same index (same db_key + idx_key combination).

    constexpr eckit::fam::size_t large_region_size = 64 * 1024 * 1024;  // 64 MB

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(large_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto db_key = fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}};
    const auto idx_key =
        fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}, {"fam2a", "d"}, {"fam2b", "e"}, {"fam2c", "f"}};

    // Mock OpenFAM has 4096 objects/region.  Each archive + flush cycle creates
    // several objects (data, FamMap/FamList nodes, axes entries).  With overhead
    // from catalogue/store infrastructure, 4 × 50 = 200 fields stays well within
    // the mock's capacity while still exercising concurrent flush races.
    constexpr int num_writers = 4;
    constexpr int fields_per_writer = 50;
    constexpr int flush_interval = 10;  // flush every N fields — maximises concurrent flush races

    const char* data = "concurrent-stress-payload";
    const auto data_length = std::char_traits<char>::length(data);

    bool ok = fork_and_run(num_writers, [&](int writer_id) {
        fdb5::FamStore fam_store(db_key, config);
        fdb5::Store& store = fam_store;
        fdb5::FamCatalogueWriter writer(db_key, config);
        fdb5::CatalogueWriter& cat_writer = writer;

        cat_writer.createIndex(idx_key, /*datumKeySize=*/3);

        for (int i = 0; i < fields_per_writer; ++i) {
            std::string fam3a_val = "w" + std::to_string(writer_id) + "f" + std::to_string(i);
            std::string fam3b_val = "lev" + std::to_string(i % 10);     // 10 shared levels
            std::string fam3c_val = "par" + std::to_string(writer_id);  // 1 per writer

            fdb5::Key datum_key{{"fam1a", "a"},       {"fam1b", "b"},       {"fam1c", "c"},
                                {"fam2a", "d"},       {"fam2b", "e"},       {"fam2c", "f"},
                                {"fam3a", fam3a_val}, {"fam3b", fam3b_val}, {"fam3c", fam3c_val}};

            auto location = store.archive(datum_key, data, eckit::Length(data_length));
            cat_writer.archive(idx_key, datum_key, location->make_shared());

            // Flush periodically — this is where the race condition matters.
            // Multiple processes flushing simultaneously must merge axes correctly.
            if ((i + 1) % flush_interval == 0) {
                cat_writer.flush(1);
            }
        }

        // Final flush for remaining fields.
        cat_writer.flush(1);
    });

    EXPECT(ok);

    // ---- Verify via persisted axes (fast path) ----
    eckit::FamRegionName root_name(fam::test_fdb_fam_endpoint, test_fdb_fam_region);
    const auto cat_name = fdb5::FamCatalogue::catalogueName(db_key);
    const auto index_name = fdb5::FamCatalogue::indexName(cat_name, idx_key);

    fdb5::FamIndex reader_idx(idx_key, root_name, index_name, /*read_axes=*/true);

    // Collect persisted axes values.
    std::set<std::string> found_fam3a;
    std::set<std::string> found_fam3b;
    std::set<std::string> found_fam3c;

    EXPECT(reader_idx.axes().has("fam3a"));
    EXPECT(reader_idx.axes().has("fam3b"));
    EXPECT(reader_idx.axes().has("fam3c"));

    for (const auto& v : reader_idx.axes().values("fam3a")) {
        found_fam3a.insert(v);
    }
    for (const auto& v : reader_idx.axes().values("fam3b")) {
        found_fam3b.insert(v);
    }
    for (const auto& v : reader_idx.axes().values("fam3c")) {
        found_fam3c.insert(v);
    }

    // Check fam3a: must have num_writers × fields_per_writer unique values.
    int missing = 0;
    for (int w = 0; w < num_writers; ++w) {
        for (int f = 0; f < fields_per_writer; ++f) {
            std::string expected = "w" + std::to_string(w) + "f" + std::to_string(f);
            if (found_fam3a.find(expected) == found_fam3a.end()) {
                if (missing < 20) {
                    eckit::Log::error() << "MISSING fam3a axes value: " << expected << " (writer " << w << ")"
                                        << std::endl;
                }
                ++missing;
            }
        }
    }

    const auto expected_fam3a = static_cast<std::size_t>(num_writers * fields_per_writer);
    eckit::Log::info() << "fam3a: found " << found_fam3a.size() << "/" << expected_fam3a << " (" << missing
                       << " missing)" << std::endl;
    EXPECT_EQUAL(missing, 0);
    EXPECT_EQUAL(found_fam3a.size(), expected_fam3a);

    // Check fam3b: 10 shared level values (lev0..lev9).
    eckit::Log::info() << "fam3b: found " << found_fam3b.size() << " values" << std::endl;
    EXPECT_EQUAL(found_fam3b.size(), 10);

    // Check fam3c: num_writers unique param values (par0..par7).
    eckit::Log::info() << "fam3c: found " << found_fam3c.size() << " values" << std::endl;
    EXPECT_EQUAL(found_fam3c.size(), static_cast<std::size_t>(num_writers));

    eckit::Log::info() << "Concurrent stress test PASSED: " << found_fam3a.size() << " unique fam3a values, "
                       << found_fam3b.size() << " fam3b values, " << found_fam3c.size() << " fam3c values from "
                       << num_writers << " writers (" << fields_per_writer << " fields each, flush every "
                       << flush_interval << ")" << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb::test

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
