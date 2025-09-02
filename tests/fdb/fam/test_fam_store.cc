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

#include <ctime>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include "eckit/config/YAMLConfiguration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/Length.h"
#include "eckit/io/fam/FamRegion.h"
#include "eckit/io/fam/FamRegionName.h"
#include "eckit/log/Log.h"
#include "eckit/testing/Test.h"

#include "fdb5/config/Config.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/Store.h"
#include "fdb5/fam/FamStore.h"
#include "fdb5/rules/Schema.h"

using namespace eckit::testing;

namespace fdb::test {

//----------------------------------------------------------------------------------------------------------------------

namespace {

constexpr eckit::fam::size_t test_region_size = 1024 * 10;  // 10 KB
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
    "catalogue: toc\n"
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

CASE("FamStore: create test region") {
    auto name = eckit::FamRegionName(test_fdb_fam_endpoint, test_fdb_fam_region);
    try {
        name.create(test_region_size, test_region_perm);
    }
    catch (const eckit::AlreadyExists& e) {
        eckit::Log::debug() << "FamCommon: " << name << " already exists.\n";
    }
}

CASE("FamStore: Archive, Retrieve, Remove") {
    TEST_LOG_DEBUG("SETUP FAM STORE");

    const fam::FamSetup setup(test_schema, test_config);

    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto& schema = config.schema();

    const auto storeKey = fdb5::Key({{"fam1a", "val1a"}, {"fam1b", "val1b"}, {"fam1c", "val1c"}});

    fdb5::FamStore famStore(storeKey, config);

    auto& store = static_cast<fdb5::Store&>(famStore);

    //------------------------------------------------------------------------------------------------------------------

    const char* value = "Testing fam: ARCHIVE DATA!";
    const auto length = std::char_traits<char>::length(value);

    auto key = fdb5::Key({{"fam1a", "val1a"},
                          {"fam1b", "val1b"},
                          {"fam1c", "val1c"},
                          {"fam2a", "val2a"},
                          {"fam2b", "val2b"},
                          {"fam2c", "val2c"},
                          {"fam3a", "val3a"},
                          {"fam3b", "val3b"},
                          {"fam3c", "val3c"}});

    //------------------------------------------------------------------------------------------------------------------

    TEST_LOG_INFO("ARCHIVE #1");

    auto floc = store.archive(key, value, length);

    EXPECT(floc);
    EXPECT_EQUAL(floc->length(), eckit::Length(length));

    // std::cerr << "Field location: " << *floc << std::endl;

    //------------------------------------------------------------------------------------------------------------------

    key = fdb5::Key({{"fam1a", "val11a"},
                     {"fam1b", "val11b"},
                     {"fam1c", "val11c"},
                     {"fam2a", "val22a"},
                     {"fam2b", "val22b"},
                     {"fam2c", "val22c"},
                     {"fam3a", "val33a"},
                     {"fam3b", "val33b"},
                     {"fam3c", "val33c"}});

    TEST_LOG_INFO("ARCHIVE #2");

    floc = store.archive(key, value, length);

    EXPECT(floc);
    EXPECT_EQUAL(floc->length(), eckit::Length(length));

    // std::cerr << "Field location: " << *floc << std::endl;

    //------------------------------------------------------------------------------------------------------------------

    TEST_LOG_INFO("RETRIEVE");

    auto field  = fdb5::Field(std::move(floc), std::time(nullptr));
    auto handle = std::unique_ptr<eckit::DataHandle>(store.retrieve(field));

    EXPECT(handle);

    // handle->seek(-length);  // seek back (because of field loc)

    fam::read_and_validate(handle.get(), value, length);

    //------------------------------------------------------------------------------------------------------------------

    TEST_LOG_INFO("REMOVE");

    store.remove(key);

    EXPECT_THROWS(fam::read_and_validate(handle.get(), value, length));

    TEST_LOG_INFO("FINISHED!");
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb::test

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
