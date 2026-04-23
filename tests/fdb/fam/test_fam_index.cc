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

/// @file   test_fam_index.cc
/// @author Metin Cakircali
/// @date   Jun 2024

#include "test_fam_common.h"

#include <cstdlib>
#include <memory>
#include <set>
#include <sstream>
#include <string>

#include "eckit/config/YAMLConfiguration.h"
#include "eckit/io/Length.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/io/fam/FamRegionName.h"
#include "eckit/runtime/Main.h"
#include "eckit/serialisation/HandleStream.h"
#include "eckit/testing/Test.h"

#include "fdb5/config/Config.h"
#include "fdb5/database/IndexLocation.h"
#include "fdb5/database/Key.h"
#include "fdb5/fam/FamCatalogue.h"
#include "fdb5/fam/FamCatalogueWriter.h"
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
const auto test_fdb_fam_region = eckit::FamPath("test_fdb_index");
const auto test_fdb_fam_uri = "fam://" + fam::test_fdb_fam_endpoint + "/" + test_fdb_fam_region.asString();

const std::string test_config = fam::make_test_config(test_fdb_fam_uri);

}  // namespace

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

//----------------------------------------------------------------------------------------------------------------------
// FamIndex: concurrent writers
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

    constexpr int num_writers = 4;
    constexpr int fields_per_writer = 50;

    bool ok = fork_and_exec(num_writers, {
                                             "--config-path=" + setup.configPath.asString(),
                                             "--fields-per-writer=" + std::to_string(fields_per_writer),
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
                       << num_writers << " writers (" << fields_per_writer << " fields each)" << std::endl;
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

}  // namespace fdb::test

//----------------------------------------------------------------------------------------------------------------------
// Child worker for fork_and_exec (concurrent writers test)
//----------------------------------------------------------------------------------------------------------------------

namespace {

int child_worker_main(int argc, char** argv) {
    eckit::Main::initialise(argc, argv);

    try {
        auto args = eckit::testing::parse_worker_args(argc, argv);
        const int writer_id = std::stoi(eckit::testing::get_worker_arg(args, "worker-id"));
        const std::string cfg_path = eckit::testing::get_worker_arg(args, "config-path");
        const int fields_per_writer = std::stoi(eckit::testing::get_worker_arg(args, "fields-per-writer"));
        constexpr int flush_interval = 10;

        const auto config = fdb5::Config{eckit::YAMLConfiguration(eckit::PathName(cfg_path))};
        const auto db_key = fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}};
        const auto idx_key =
            fdb5::Key{{"fam1a", "a"}, {"fam1b", "b"}, {"fam1c", "c"}, {"fam2a", "d"}, {"fam2b", "e"}, {"fam2c", "f"}};

        const char* data = "concurrent-stress-payload";
        const auto data_length = std::char_traits<char>::length(data);

        fdb5::FamStore fam_store(db_key, config);
        fdb5::Store& store = fam_store;
        fdb5::FamCatalogueWriter writer(db_key, config);
        fdb5::CatalogueWriter& cat_writer = writer;

        cat_writer.createIndex(idx_key, /*datumKeySize=*/3);

        for (int i = 0; i < fields_per_writer; ++i) {
            std::string fam3a_val = "w" + std::to_string(writer_id) + "f" + std::to_string(i);
            std::string fam3b_val = "lev" + std::to_string(i % 10);
            std::string fam3c_val = "par" + std::to_string(writer_id);

            fdb5::Key datum_key{{"fam1a", "a"},       {"fam1b", "b"},       {"fam1c", "c"},
                                {"fam2a", "d"},       {"fam2b", "e"},       {"fam2c", "f"},
                                {"fam3a", fam3a_val}, {"fam3b", fam3b_val}, {"fam3c", fam3c_val}};

            auto location = store.archive(datum_key, data, eckit::Length(data_length));
            cat_writer.archive(idx_key, datum_key, location->make_shared());

            if ((i + 1) % flush_interval == 0) {
                cat_writer.flush(1);
            }
        }

        cat_writer.flush(1);
        return 0;
    }
    catch (const std::exception& e) {
        eckit::Log::error() << "Child worker FAILED: " << e.what() << std::endl;
        return 1;
    }
}

}  // namespace

int main(int argc, char** argv) {
    auto args = eckit::testing::parse_worker_args(argc, argv);
    if (!args.empty()) {
        return child_worker_main(argc, argv);
    }
    return eckit::testing::run_tests(argc, argv);
}
