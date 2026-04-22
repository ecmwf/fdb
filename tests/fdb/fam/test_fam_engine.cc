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

/// @file   test_fam_engine.cc
/// @author Metin Cakircali
/// @date   Jun 2024

#include "test_fam_common.h"

#include <string>

#include "eckit/config/YAMLConfiguration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/io/fam/FamRegionName.h"
#include "eckit/testing/Test.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/config/Config.h"
#include "fdb5/database/Engine.h"
#include "fdb5/database/Key.h"
#include "fdb5/fam/FamCatalogueWriter.h"
#include "fdb5/fam/FamEngine.h"

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

}  // namespace fdb::test

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
