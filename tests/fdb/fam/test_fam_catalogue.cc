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
#include "eckit/io/fam/FamProperty.h"
#include "eckit/io/fam/FamRegion.h"
#include "eckit/io/fam/FamRegionName.h"
#include "eckit/log/Log.h"
#include "eckit/testing/Test.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/Callback.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/Key.h"
#include "fdb5/fam/FamCatalogue.h"
#include "fdb5/fam/FamCatalogueReader.h"
#include "fdb5/fam/FamCatalogueWriter.h"
#include "fdb5/fam/FamCommon.h"
#include "fdb5/fam/FamStore.h"
#include "fdb5/rules/Schema.h"

using namespace eckit::testing;

namespace fdb::test {

//----------------------------------------------------------------------------------------------------------------------

namespace {

constexpr eckit::fam::size_t test_region_size = 1024 * 1024;  // 1 MB
constexpr eckit::fam::perm_t test_region_perm = 0640;

const std::string test_schema =
    "[ fam1a, fam1b, fam1c\n"
    "    [ fam2a, fam2b, fam2c\n"
    "       [ fam3a, fam3b, fam3c ]]]\n";

const std::string test_config =
    "---\n"
    "type: local\n"
    "schema: ./schema\n"
    "store: fam\n"
    "catalogue: fam\n"
    "spaces:\n"
    "- handler: Default\n"
    "  roots:\n"
    "  - path: ./root\n"
    "fam_roots:\n"
    "- uri: " +
    fam::test_fdb_fam_uri +
    "\n"
    "  writable: true\n"
    "  visit: true\n";

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

CASE("FamCatalogueWriter/Reader: direct OpenFAM metadata roundtrip") {

    const auto cat_region = eckit::FamPath("test_region_fdb_catalog");

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, cat_region).create(test_region_size, test_region_perm, true);

    std::string cat_config = test_config;
    {
        const auto uri_pos = cat_config.find(fam::test_fdb_fam_uri);
        EXPECT_NOT_EQUAL(uri_pos, std::string::npos);
        const auto direct_uri = "fam://" + fam::test_fdb_fam_endpoint + "/" + cat_region.asString();
        cat_config.replace(uri_pos, fam::test_fdb_fam_uri.size(), direct_uri);
    }

    const fam::FamSetup setup(test_schema, cat_config);
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

    const char* data       = "Testing fam: ARCHIVE DATA!";
    const auto data_length = std::char_traits<char>::length(data);

    fdb5::FamStore fam_store(db_key, config);
    auto& store   = static_cast<fdb5::Store&>(fam_store);
    auto location = store.archive(datum_key, data, eckit::Length(data_length));
    EXPECT(location);

    fdb5::FamCatalogueWriter writer(db_key, config);
    fdb5::Catalogue& writer_cat         = writer;
    fdb5::CatalogueWriter& writer_iface = writer;

    EXPECT_EQUAL(writer_cat.type(), std::string("fam"));
    EXPECT_EQUAL(writer_cat.uri().scheme(), std::string("fam"));
    EXPECT(writer_cat.exists());

    EXPECT(writer_iface.createIndex(idx_key, datum_key.size()));
    writer_iface.archive(idx_key, datum_key, location->make_shared());
    writer_iface.flush(1);

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

    const auto root = eckit::FamRegionName(fam::test_fdb_fam_endpoint, cat_region);

    const auto cat_map_name = fdb5::FamCatalogue::catalogueName(db_key) + fdb5::FamCommon::table_suffix;
    const auto idx_map_name = fdb5::FamCatalogue::indexName(idx_key) + fdb5::FamCommon::table_suffix;
    const auto reg_map_name = std::string("fdb-reg") + fdb5::FamCommon::table_suffix;

    EXPECT(root.object(reg_map_name).exists());
    EXPECT(root.object(cat_map_name).exists());
    EXPECT(root.object(idx_map_name).exists());
}

CASE("FamCatalogue: Archive, Retrieve, Remove") {
    TEST_LOG_DEBUG("SETUP FDB FAM");

    const fam::FamSetup setup(test_schema, test_config);

    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    fdb5::FDB fdb(config);

    //------------------------------------------------------------------------------------------------------------------

    const char* data       = "Testing fam: ARCHIVE DATA!";
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
            auto list  = fdb.list(request);
            auto count = fam::count_list(list);
            EXPECT_EQUAL(count, 0);
        }

        //------------------------------------------------------------------------------------------------------------------

        fdb.archive(key, data, data_length);
        fdb.flush();

        {
            TEST_LOG_INFO("LIST FIRST ARCHIVED DATA");
            auto list  = fdb.list(request);
            auto count = fam::count_list(list);
            EXPECT_EQUAL(count, 1);
        }
    }

    //------------------------------------------------------------------------------------------------------------------

    {
        TEST_LOG_INFO("ARCHIVE #2");

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

        auto list  = fdb.list(request);
        auto count = fam::count_list(list);
        EXPECT_EQUAL(count, 1);
    }

    //------------------------------------------------------------------------------------------------------------------

    {
        TEST_LOG_INFO("LIST BOTH ARCHIVED DATA");
        auto key           = fdb5::Key({
            {"fam1a", "val1a"},
            {"fam1b", "val1b"},
            {"fam1c", "val1c"},
            {"fam2a", "val2a"},
            {"fam2b", "val2b"},
            {"fam2c", "val2c"},
        });
        const auto request = key.request("retrieve");
        auto list          = fdb.list(request);
        auto count         = fam::count_list(list);
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

    // handle->seek(-length);  // seek back (because of field loc)
    // fam::readAndValidate(handle.get(), value, length);

    //     //------------------------------------------------------------------------------------------------------------------
    //
    //     TEST_LOG_INFO("REMOVE");
    //
    //     store.remove(key);
    //
    //     EXPECT_THROWS(fam::readAndValidate(handle.get(), value, length));

    TEST_LOG_INFO("FINISHED!");
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb::test

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
