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

#include "eckit/config/YAMLConfiguration.h"
#include "eckit/io/Length.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/io/fam/FamRegionName.h"
#include "eckit/testing/Test.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Engine.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/WipeState.h"
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

const std::string test_config = fam::make_test_config(test_fdb_fam_uri);

}  // namespace

//----------------------------------------------------------------------------------------------------------------------
// FamCatalogue: naming helpers and metadata roundtrip
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

//----------------------------------------------------------------------------------------------------------------------
// Schema persistence
//----------------------------------------------------------------------------------------------------------------------

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

//----------------------------------------------------------------------------------------------------------------------
// Multiple indexes
//----------------------------------------------------------------------------------------------------------------------

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

//----------------------------------------------------------------------------------------------------------------------
// Wipe operations
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
// FamCatalogue: NOTIMP methods and stubs
//----------------------------------------------------------------------------------------------------------------------

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

}  // namespace fdb::test

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
