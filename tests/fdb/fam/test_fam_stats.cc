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

/// @file   test_fam_stats.cc
/// @author Metin Cakircali
/// @date   Jul 2026

#include "test_fam_common.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/PurgeIterator.h"
#include "fdb5/api/helpers/StatsIterator.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/Store.h"
#include "fdb5/fam/FamCatalogueWriter.h"
#include "fdb5/fam/FamStore.h"

#include "eckit/config/YAMLConfiguration.h"
#include "eckit/io/Length.h"
#include "eckit/io/fam/FamRegion.h"
#include "eckit/io/fam/FamRegionName.h"
#include "eckit/testing/Test.h"

#include <cstring>
#include <sstream>
#include <string>

namespace fdb::test {

//----------------------------------------------------------------------------------------------------------------------

namespace {

constexpr eckit::fam::size_t test_region_size = 1024 * 1024;  // 1 MB
constexpr eckit::fam::perm_t test_region_perm = 0640;
const auto test_fdb_fam_region = eckit::FamPath("test_fdb_stats");
const auto test_fdb_fam_uri = "fam://" + fam::test_fdb_fam_endpoint + "/" + test_fdb_fam_region.asString();

const std::string test_config = fam::make_test_config(test_fdb_fam_uri);

fdb5::Key makeDatumKey(const std::string& v3c) {
    return fdb5::Key({{"fam1a", "val1a"},
                      {"fam1b", "val1b"},
                      {"fam1c", "val1c"},
                      {"fam2a", "val2a"},
                      {"fam2b", "val2b"},
                      {"fam2c", "val2c"},
                      {"fam3a", "val3a"},
                      {"fam3b", "val3b"},
                      {"fam3c", v3c}});
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

CASE("FamStats: fdb.stats reports fields, duplicates and sizes") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    fdb5::FDB fdb(config);

    // Two distinct fields (A, B) sharing the same index, plus a re-archive of A (duplicate).
    const std::string data_a(16, 'a');   // field A, first version
    const std::string data_a2(16, 'A');  // field A, superseding version (same length)
    const std::string data_b(24, 'b');   // field B

    const auto key_a = makeDatumKey("val3ca");
    const auto key_b = makeDatumKey("val3cb");

    fdb.archive(key_a, data_a.data(), data_a.size());
    fdb.archive(key_b, data_b.data(), data_b.size());
    fdb.flush();

    // Re-archive A: forceInsert appends, so this supersedes the first A (older becomes a duplicate).
    fdb.archive(key_a, data_a2.data(), data_a2.size());
    fdb.flush();

    const fdb5::FDBToolRequest request(makeDatumKey("val3ca").request("retrieve"), false,
                                       std::vector<std::string>{"fam1a", "fam1b", "fam1c"});

    auto stats = fdb.stats(request);

    size_t total_fields = 0;
    size_t total_duplicates = 0;
    size_t total_fields_size = 0;
    size_t total_duplicates_size = 0;

    fdb5::StatsElement elem;
    size_t db_count = 0;
    std::ostringstream db_report;
    while (stats.next(elem)) {
        ++db_count;
        total_fields += elem.indexStatistics.fieldsCount();
        total_duplicates += elem.indexStatistics.duplicatesCount();
        total_fields_size += elem.indexStatistics.fieldsSize();
        total_duplicates_size += elem.indexStatistics.duplicatesSize();
        elem.dbStatistics.report(db_report);
    }

    EXPECT_EQUAL(db_count, 1U);
    EXPECT_EQUAL(total_fields, 3U);      // A(v1), B, A(v2)
    EXPECT_EQUAL(total_duplicates, 1U);  // superseded A(v1)
    EXPECT_EQUAL(total_fields_size, data_a.size() + data_b.size() + data_a2.size());
    EXPECT_EQUAL(total_duplicates_size, data_a.size());

    // DB statistics distinguish total vs reachable data size, and count the index.
    const auto report = db_report.str();
    EXPECT(report.find("Indexes") != std::string::npos);
    EXPECT(report.find("Total data size") != std::string::npos);
    EXPECT(report.find("Reachable data size") != std::string::npos);
}

//----------------------------------------------------------------------------------------------------------------------

CASE("FamPurge: removes superseded data objects and keeps live data") {

    eckit::FamRegionName(fam::test_fdb_fam_endpoint, test_fdb_fam_region)
        .create(test_region_size, test_region_perm, true);

    const fam::FamSetup setup(fam::test_schema, test_config);
    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto db_key = fdb5::Key{{"fam1a", "val1a"}, {"fam1b", "val1b"}, {"fam1c", "val1c"}};
    const auto idx_key = fdb5::Key{{"fam1a", "val1a"}, {"fam1b", "val1b"}, {"fam1c", "val1c"},
                                   {"fam2a", "val2a"}, {"fam2b", "val2b"}, {"fam2c", "val2c"}};
    const auto datum_key = makeDatumKey("val3ca");

    const std::string data_v1(16, '1');
    const std::string data_v2(16, '2');

    // Archive two versions of the same field: v2 supersedes v1, leaving v1's object unreferenced.
    fdb5::FamStore fam_store(db_key, config);
    fdb5::Store& store = fam_store;
    auto loc1 = store.archive(datum_key, data_v1.data(), data_v1.size());
    auto loc2 = store.archive(datum_key, data_v2.data(), data_v2.size());
    const auto uri1 = loc1->uri();
    const auto uri2 = loc2->uri();

    {
        fdb5::FamCatalogueWriter writer(db_key, config);
        fdb5::CatalogueWriter& writer_iface = writer;
        writer_iface.archive(idx_key, datum_key, loc1->make_shared());
        writer_iface.flush(1);
        writer_iface.archive(idx_key, datum_key, loc2->make_shared());  // supersedes v1
        writer_iface.flush(1);
    }

    EXPECT(fam_store.uriExists(uri1));
    EXPECT(fam_store.uriExists(uri2));

    fdb5::FDB fdb(config);
    const fdb5::FDBToolRequest request(db_key.request("retrieve"), false,
                                       std::vector<std::string>{"fam1a", "fam1b", "fam1c"});

    // Dry-run purge: reports but removes nothing.
    {
        auto purge = fdb.purge(request, false, false);
        fdb5::PurgeElement elem;
        while (purge.next(elem)) {}
    }
    EXPECT(fam_store.uriExists(uri1));
    EXPECT(fam_store.uriExists(uri2));

    // Commit purge: the superseded object (v1) is removed, the live object (v2) is kept.
    {
        auto purge = fdb.purge(request, true, false);
        fdb5::PurgeElement elem;
        while (purge.next(elem)) {}
    }
    EXPECT(!fam_store.uriExists(uri1));
    EXPECT(fam_store.uriExists(uri2));
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb::test

int main(int argc, char** argv) {
    return eckit::testing::run_tests(argc, argv);
}
