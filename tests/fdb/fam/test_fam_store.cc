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

#include "eckit/io/fam/FamRegion.h"
#include "eckit/testing/Test.h"

#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"
#include "fdb5/fam/FamStore.h"
#include "fdb5/rules/Schema.h"

using namespace eckit::testing;

namespace fdb::test {

//----------------------------------------------------------------------------------------------------------------------

namespace {

const std::string testSchema =
    "[ fam1a, fam1b, fam1c\n"
    "    [ fam2a, fam2b, fam2c\n"
    "       [ fam3a, fam3b, fam3c ]]]\n";

const std::string testConfig =
    "---\n"
    "type: local\n"
    "schema: ./schema\n"
    "store: fam\n"
    "catalogue: toc\n"
    "spaces:\n"
    "- handler: Default\n"
    "  roots:\n"
    "  - path: ./root\n"
    "  - path: fam://endpoint/region\n"
    "  - path: ./backup\n"
    "    wipe: true\n"
    "    list: true\n"
    "    retrieve: false\n"
    "fam_roots:\n"
    "- uri: " +
    TEST_FDB_FAM_URI +
    "\n"
    "  writable: true\n"
    "  visit: true\n";

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

CASE("FamStore: create test region") {
    auto name = eckit::FamRegionName(TEST_FDB_FAM_ENDPOINT, TEST_FDB_FAM_REGION);
    try {
        name.create(1024 * 4, 0640);
    }
    catch (const eckit::AlreadyExists& e) {
        eckit::Log::debug() << "FamCommon: " << name << " already exists.\n";
    }
}

CASE("FamStore: Archive, Retrieve, Remove") {
    TEST_LOG_DEBUG("SETUP FAM STORE");

    const fam::FamSetup setup(testSchema, testConfig);

    const auto config = fdb5::Config{eckit::YAMLConfiguration(setup.configPath)};

    const auto& schema = config.schema();

    const auto storeKey = fdb5::Key({{"fam1a", "val1a"}, {"fam1b", "val1b"}, {"fam1c", "val1c"}}, schema.registry());

    fdb5::FamStore famStore(schema, storeKey, config);

    auto& store = static_cast<fdb5::Store&>(famStore);

    //------------------------------------------------------------------------------------------------------------------

    const char* value = "Testing fam: ARCHIVE DATA!";
    const auto length = std::char_traits<char>::length(value);

    const auto key = fdb5::Key({{"fam1a", "val1a"},
                                {"fam1b", "val1b"},
                                {"fam1c", "val1c"},
                                {"fam2a", "val2a"},
                                {"fam2b", "val2b"},
                                {"fam2c", "val2c"},
                                {"fam3a", "val3a"},
                                {"fam3b", "val3b"},
                                {"fam3c", "val3c"}},
                               schema.registry());

    //------------------------------------------------------------------------------------------------------------------

    TEST_LOG_INFO("ARCHIVE");

    auto floc = store.archive(key, value, length);

    EXPECT(floc && floc->length() == eckit::Length(length));

    //------------------------------------------------------------------------------------------------------------------

    TEST_LOG_INFO("RETRIEVE");

    auto field  = fdb5::Field(std::move(floc), std::time(nullptr));
    auto handle = std::unique_ptr<eckit::DataHandle>(store.retrieve(field));

    EXPECT(handle);

    // handle->seek(-length);  // seek back (because of field loc)

    fam::readAndValidate(handle.get(), value, length);

    //------------------------------------------------------------------------------------------------------------------

    TEST_LOG_INFO("REMOVE");

    store.remove(key);

    EXPECT_THROWS(fam::readAndValidate(handle.get(), value, length));

    TEST_LOG_INFO("FINISHED!");
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb::test

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
