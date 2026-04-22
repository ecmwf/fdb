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

/// @file   test_fam_store.cc
/// @author Metin Cakircali
/// @date   Jun 2024

#include "test_fam_common.h"

#include <cstring>
#include <ctime>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <utility>

#include "eckit/config/YAMLConfiguration.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/Length.h"
#include "eckit/io/fam/FamRegionName.h"
#include "eckit/testing/Test.h"

#include "fdb5/config/Config.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/Store.h"
#include "fdb5/database/WipeState.h"
#include "fdb5/fam/FamFieldLocation.h"
#include "fdb5/fam/FamStore.h"
#include "fdb5/rules/Schema.h"

namespace fdb::test {

//----------------------------------------------------------------------------------------------------------------------

namespace {

constexpr eckit::fam::size_t test_region_size = 1024 * 64;  // 64 KB
constexpr eckit::fam::perm_t test_region_perm = 0640;
const auto test_fdb_fam_region = eckit::FamPath("test_fdb_store");
const auto test_fdb_fam_uri = "fam://" + fam::test_fdb_fam_endpoint + "/" + test_fdb_fam_region.asString();

const std::string test_config = fam::make_test_config(test_fdb_fam_uri, "toc", "");


}  // namespace

//----------------------------------------------------------------------------------------------------------------------

CASE("FamStore: metadata and identity") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto store_key = fdb5::Key({{"fam1a", "v1a"}, {"fam1b", "v1b"}, {"fam1c", "v1c"}});

    fdb5::FamStore fam_store(store_key, config);
    fdb5::Store& store = fam_store;

    EXPECT_EQUAL(store.type(), std::string("fam"));
    EXPECT_EQUAL(store.uri().scheme(), std::string("fam"));
    EXPECT(store.exists());

    // Collocated URIs start empty (no sub-objects tracked)
    EXPECT(store.collocatedDataURIs().empty());

    // flush() returns zero when nothing has been archived
    EXPECT_EQUAL(store.flush(), 0U);
}

CASE("FamStore: archive, retrieve, flush") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto store_key = fdb5::Key({{"fam1a", "v1a"}, {"fam1b", "v1b"}, {"fam1c", "v1c"}});

    fdb5::FamStore fam_store(store_key, config);
    fdb5::Store& store = fam_store;

    //------------------------------------------------------------------------------------------------------------------

    const char* data1 = "Testing fam: ARCHIVE DATA #1";
    const auto data1_length = std::char_traits<char>::length(data1);

    const char* data2 = "Different payload for field #2";
    const auto data2_length = std::char_traits<char>::length(data2);

    auto key1 = fdb5::Key({{"fam1a", "v1a"},
                           {"fam1b", "v1b"},
                           {"fam1c", "v1c"},
                           {"fam2a", "v2a"},
                           {"fam2b", "v2b"},
                           {"fam2c", "v2c"},
                           {"fam3a", "v3a"},
                           {"fam3b", "v3b"},
                           {"fam3c", "v3c"}});

    auto key2 = fdb5::Key({{"fam1a", "v1a"},
                           {"fam1b", "v1b"},
                           {"fam1c", "v1c"},
                           {"fam2a", "v2a"},
                           {"fam2b", "v2b"},
                           {"fam2c", "v2c"},
                           {"fam3a", "v3a"},
                           {"fam3b", "v3b"},
                           {"fam3c", "x3c"}});

    //------------------------------------------------------------------------------------------------------------------

    TEST_LOG_INFO("ARCHIVE #1");

    auto floc1 = store.archive(key1, data1, data1_length);
    EXPECT(floc1);
    EXPECT_EQUAL(floc1->length(), eckit::Length(data1_length));

    TEST_LOG_INFO("ARCHIVE #2");

    auto floc2 = store.archive(key2, data2, data2_length);
    EXPECT(floc2);
    EXPECT_EQUAL(floc2->length(), eckit::Length(data2_length));

    // Archived URIs must be distinct (UUID-based)
    EXPECT(floc1->uri() != floc2->uri());

    //------------------------------------------------------------------------------------------------------------------

    TEST_LOG_INFO("FLUSH — returns archived count");

    EXPECT_EQUAL(store.flush(), 2U);
    // Second flush returns zero — counter was reset
    EXPECT_EQUAL(store.flush(), 0U);

    //------------------------------------------------------------------------------------------------------------------

    TEST_LOG_INFO("URI MEMBERSHIP");

    EXPECT(store.uriBelongs(floc1->uri()));
    EXPECT(store.uriExists(floc1->uri()));
    EXPECT(store.uriBelongs(floc2->uri()));
    EXPECT(store.uriExists(floc2->uri()));

    // A fabricated URI in the same scheme but different region should not belong
    const eckit::URI foreign("fam://host:1234/other_region/obj");
    EXPECT(!store.uriBelongs(foreign));

    //------------------------------------------------------------------------------------------------------------------

    TEST_LOG_INFO("RETRIEVE #1");
    {
        auto field = fdb5::Field(std::move(floc1), std::time(nullptr));
        auto handle = std::unique_ptr<eckit::DataHandle>(store.retrieve(field));
        EXPECT(handle);
        fam::read_and_validate(handle.get(), data1, data1_length);
    }

    TEST_LOG_INFO("RETRIEVE #2 — different payload");
    {
        auto field = fdb5::Field(std::move(floc2), std::time(nullptr));
        auto handle = std::unique_ptr<eckit::DataHandle>(store.retrieve(field));
        EXPECT(handle);
        fam::read_and_validate(handle.get(), data2, data2_length);
    }

    //------------------------------------------------------------------------------------------------------------------

    TEST_LOG_INFO("CLOSE");
    store.close();

    TEST_LOG_INFO("FINISHED!");
}

CASE("FamStore: URI-based construction") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    // Construct from URI instead of Key
    const eckit::URI root_uri(test_fdb_fam_uri);
    fdb5::FamStore fam_store(root_uri, config);
    fdb5::Store& store = fam_store;

    EXPECT_EQUAL(store.type(), std::string("fam"));
    EXPECT(store.exists());

    // Archive a field to verify the URI-constructed store is fully functional
    const char* data = "URI-constructed store data";
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
    EXPECT_EQUAL(floc->length(), eckit::Length(data_length));

    auto field = fdb5::Field(std::move(floc), std::time(nullptr));
    auto handle = std::unique_ptr<eckit::DataHandle>(store.retrieve(field));
    EXPECT(handle);
    fam::read_and_validate(handle.get(), data, data_length);

    TEST_LOG_INFO("FINISHED!");
}

CASE("FamStore: asCollocatedDataURIs validates URIs") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto store_key = fdb5::Key({{"fam1a", "v1a"}, {"fam1b", "v1b"}, {"fam1c", "v1c"}});

    fdb5::FamStore fam_store(store_key, config);
    fdb5::Store& store = fam_store;

    // Archive to get a valid URI
    const char* data = "collocated-test-data";
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

    // Valid URI should pass through
    std::set<eckit::URI> valid_uris{floc->uri()};
    auto result = store.asCollocatedDataURIs(valid_uris);
    EXPECT_EQUAL(result.size(), 1U);

    // Empty set should return empty set
    std::set<eckit::URI> empty_uris;
    EXPECT(store.asCollocatedDataURIs(empty_uris).empty());

    // Foreign URI should throw
    std::set<eckit::URI> foreign_uris{eckit::URI("fam://other:1234/foreign_region/obj")};
    EXPECT_THROWS_AS(store.asCollocatedDataURIs(foreign_uris), eckit::BadValue);
}

CASE("FamStore: getAuxiliaryURIs returns empty") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto store_key = fdb5::Key({{"fam1a", "v1a"}, {"fam1b", "v1b"}, {"fam1c", "v1c"}});

    fdb5::FamStore fam_store(store_key, config);

    const auto fam_uri = eckit::URI(test_fdb_fam_uri + "/some_object");
    auto aux = fam_store.getAuxiliaryURIs(fam_uri, true);
    EXPECT(aux.empty());

    auto aux2 = fam_store.getAuxiliaryURIs(fam_uri, false);
    EXPECT(aux2.empty());
}

CASE("FamStore: print outputs store info") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto store_key = fdb5::Key({{"fam1a", "v1a"}, {"fam1b", "v1b"}, {"fam1c", "v1c"}});

    fdb5::FamStore fam_store(store_key, config);

    std::ostringstream out;
    out << fam_store;
    EXPECT(out.str().find("FamStore") != std::string::npos);
}

CASE("FamStore: remove deallocates archived object") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto store_key = fdb5::Key({{"fam1a", "v1a"}, {"fam1b", "v1b"}, {"fam1c", "v1c"}});

    fdb5::FamStore fam_store(store_key, config);
    fdb5::Store& store = fam_store;

    const char* data = "data-to-remove";
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
    EXPECT(store.uriExists(floc->uri()));

    // remove with doit=true
    std::ostringstream log_always, log_verbose;
    store.remove(floc->uri(), log_always, log_verbose, true);

    EXPECT(!store.uriExists(floc->uri()));

    // Removing again should not throw (already gone)
    EXPECT_NO_THROW(store.remove(floc->uri(), log_always, log_verbose, true));

    // remove with doit=false (dry run): should not throw
    auto floc2 = store.archive(key, data, data_length);
    EXPECT(floc2);
    store.remove(floc2->uri(), log_always, log_verbose, false);
    // Object should still exist after dry run
    EXPECT(store.uriExists(floc2->uri()));
}

CASE("FamStore: doUnsafeFullWipe returns false") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto store_key = fdb5::Key({{"fam1a", "v1a"}, {"fam1b", "v1b"}, {"fam1c", "v1c"}});

    fdb5::FamStore fam_store(store_key, config);

    EXPECT(!fam_store.doUnsafeFullWipe());
}

CASE("FamStore: doWipeEmptyDatabase is a no-op") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto store_key = fdb5::Key({{"fam1a", "v1a"}, {"fam1b", "v1b"}, {"fam1c", "v1c"}});

    fdb5::FamStore fam_store(store_key, config);

    EXPECT_NO_THROW(fam_store.doWipeEmptyDatabase());
}

CASE("FamStore: doWipeUnknowns removes matching objects") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto store_key = fdb5::Key({{"fam1a", "v1a"}, {"fam1b", "v1b"}, {"fam1c", "v1c"}});

    fdb5::FamStore fam_store(store_key, config);
    fdb5::Store& store = fam_store;

    const char* data = "unknown-wipe-data";
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

    std::set<eckit::URI> unknowns{floc->uri()};
    EXPECT(fam_store.doWipeUnknowns(unknowns));
    EXPECT(!store.uriExists(floc->uri()));

    // Foreign URIs are skipped but don't cause failure
    std::set<eckit::URI> foreign{eckit::URI("fam://other:1234/foreign_region/obj")};
    EXPECT(fam_store.doWipeUnknowns(foreign));
}

CASE("FamStore: doWipeURIs removes included and auxiliary URIs") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto store_key = fdb5::Key({{"fam1a", "v1a"}, {"fam1b", "v1b"}, {"fam1c", "v1c"}});

    fdb5::FamStore fam_store(store_key, config);
    fdb5::Store& store = fam_store;

    const char* data = "wipe-uris-data";
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

    fdb5::StoreWipeState wipeState(fam_store.uri());
    wipeState.includeData(floc->uri());

    EXPECT(fam_store.doWipeURIs(wipeState));
    EXPECT(!store.uriExists(floc->uri()));
}

CASE("FamStore: finaliseWipeState with empty URIs returns immediately") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto store_key = fdb5::Key({{"fam1a", "v1a"}, {"fam1b", "v1b"}, {"fam1c", "v1c"}});

    fdb5::FamStore fam_store(store_key, config);

    fdb5::StoreWipeState wipeState(fam_store.uri());
    EXPECT_NO_THROW(fam_store.finaliseWipeState(wipeState, false, false));
}

CASE("FamStore: finaliseWipeState with existing URI") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto store_key = fdb5::Key({{"fam1a", "v1a"}, {"fam1b", "v1b"}, {"fam1c", "v1c"}});

    fdb5::FamStore fam_store(store_key, config);
    fdb5::Store& store = fam_store;

    const char* data = "finalise-data";
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

    fdb5::StoreWipeState wipeState(fam_store.uri());
    wipeState.includeData(floc->uri());
    fam_store.finaliseWipeState(wipeState, true, false);
    EXPECT(wipeState.missingURIs().empty());
}

CASE("FamStore: checkUID is a no-op") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto store_key = fdb5::Key({{"fam1a", "v1a"}, {"fam1b", "v1b"}, {"fam1c", "v1c"}});

    fdb5::FamStore fam_store(store_key, config);
    EXPECT_NO_THROW(fam_store.checkUID());
}

CASE("FamStore: open returns true") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto store_key = fdb5::Key({{"fam1a", "v1a"}, {"fam1b", "v1b"}, {"fam1c", "v1c"}});

    fdb5::FamStore fam_store(store_key, config);
    EXPECT(fam_store.open());
}

CASE("FamStore: static uri resolves correctly") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const eckit::URI fam_uri(test_fdb_fam_uri + "/some_object");
    auto resolved = fdb5::FamStore::uri(fam_uri);
    EXPECT_EQUAL(resolved.scheme(), std::string("fam"));
}

CASE("FamStore: finaliseWipeState throws on non-belonging URI") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto store_key = fdb5::Key({{"fam1a", "v1a"}, {"fam1b", "v1b"}, {"fam1c", "v1c"}});

    fdb5::FamStore fam_store(store_key, config);

    // Include a URI that doesn't belong to this store
    fdb5::StoreWipeState wipeState(fam_store.uri());
    wipeState.includeData(eckit::URI("fam://other:1234/foreign_region/obj"));
    EXPECT_THROWS_AS(fam_store.finaliseWipeState(wipeState, true, false), eckit::SeriousBug);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb::test

int main(int argc, char** argv) {
    return eckit::testing::run_tests(argc, argv);
}
