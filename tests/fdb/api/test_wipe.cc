/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <functional>

#include "eckit/config/YAMLConfiguration.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/TmpFile.h"
#include "eckit/io/FileHandle.h"
#include "eckit/testing/Filesystem.h"
#include "eckit/testing/Test.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/config/Config.h"

using namespace eckit::testing;
using namespace eckit;

// temporary schema,spaces,root files common to all DAOS Store tests

eckit::TmpFile& schema_file() {
    static eckit::TmpFile f{};
    return f;
}

eckit::PathName& wipe_tests_tmp_root() {
    static eckit::PathName wipeRoot("./wipe_tests_fdb_root");
    return wipeRoot;
}

eckit::PathName& wipe_tests_tmp_root_store() {
    static eckit::PathName wipeRootStore("./wipe_tests_fdb_root_store");
    return wipeRootStore;
}

size_t countAll(fdb5::FDB& fdb, const std::vector<std::reference_wrapper<fdb5::FDBToolRequest>> reqs,
                bool deduplicate = false) {
    size_t count = 0;
    fdb5::ListElement info;

    for (const auto& req : reqs) {
        auto listObject = fdb.list(req, deduplicate);
        while (listObject.next(info)) {
            info.print(std::cout, true, true, false, " ");
            std::cout << std::endl;
            ++count;
        }
    }

    return count;
}

std::string configName;
fdb5::Config config;

namespace fdb5 {
namespace test {

CASE("Wipe tests") {

    // ensure fdb root directory exists. If not, then that root is
    // registered as non existing and Store tests fail.
    if (wipe_tests_tmp_root().exists()) {
        testing::deldir(wipe_tests_tmp_root());
    }
    wipe_tests_tmp_root().mkdir();

    if (wipe_tests_tmp_root_store().exists()) {
        testing::deldir(wipe_tests_tmp_root_store());
    }
    wipe_tests_tmp_root_store().mkdir();

    // prepare schema

    std::string schemaStr{"[ a, b [ c, d [ e, f ]]]"};

    std::unique_ptr<eckit::DataHandle> hs(schema_file().fileHandle());
    hs->openForWrite(schemaStr.size());
    {
        eckit::AutoClose closer(*hs);
        hs->write(schemaStr.data(), schemaStr.size());
    }

    // this is necessary to avoid ~fdb/etc/fdb/schema being used where
    // LibFdb5::instance().defaultConfig().schema() is called
    // due to no specified schema file (e.g. in Key::registry())
    ::setenv("FDB_SCHEMA_FILE", schema_file().path().c_str(), 1);

    // request

    Key requestKey1({{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"}, {"e", "5"}, {"f", "6"}});
    Key dbKey1({{"a", "1"}, {"b", "2"}});
    Key indexKey1({{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"}});

    FDBToolRequest fullReq1{requestKey1.request("retrieve"), false, std::vector<std::string>{"a", "b"}};
    FDBToolRequest indexReq1{indexKey1.request("retrieve"), false, std::vector<std::string>{"a", "b"}};
    FDBToolRequest dbReq1{dbKey1.request("retrieve"), false, std::vector<std::string>{"a", "b"}};

    Key requestKey2({{"a", "1"}, {"b", "y"}, {"c", "z"}, {"d", "t"}, {"e", "u"}, {"f", "v"}});
    Key dbKey2({{"a", "1"}, {"b", "y"}});
    Key indexKey2({{"a", "1"}, {"b", "y"}, {"c", "z"}, {"d", "t"}});

    FDBToolRequest fullReq2{requestKey2.request("retrieve"), false, std::vector<std::string>{"a", "b"}};
    FDBToolRequest indexReq2{indexKey2.request("retrieve"), false, std::vector<std::string>{"a", "b"}};
    FDBToolRequest dbReq2{dbKey2.request("retrieve"), false, std::vector<std::string>{"a", "b"}};

    Key requestKey3({{"a", "1"}, {"b", "y"}, {"c", "abc"}, {"d", "t"}, {"e", "u"}, {"f", "v"}});
    Key dbKey3({{"a", "1"}, {"b", "y"}});
    Key indexKey3({{"a", "1"}, {"b", "y"}, {"c", "abc"}, {"d", "t"}});

    FDBToolRequest fullReq3{requestKey3.request("retrieve"), false, std::vector<std::string>{"a", "b"}};
    FDBToolRequest indexReq3{indexKey3.request("retrieve"), false, std::vector<std::string>{"a", "b"}};
    FDBToolRequest dbReq3{dbKey3.request("retrieve"), false, std::vector<std::string>{"a", "b"}};

    Key commonKey({{"a", "1"}});
    FDBToolRequest commonReq{commonKey.request("retrieve"), false, std::vector<std::string>{"a"}};

    // dummy field data

    char data[] = "test";

    SECTION("WIPE MULTIPLE DATABASES; DRY-RUN WIPE") {

        // initialise FDB

        FDB fdb(config);

        // check FDB is empty

        EXPECT_EQUAL(countAll(fdb, {commonReq}), 0);
        std::cout << "Listed 0 fields" << std::endl;

        // store data

        fdb.archive(requestKey1, data, sizeof(data));
        fdb.archive(requestKey2, data, sizeof(data));
        std::cout << "Archived 2 fields on 2 databases" << std::endl;

        fdb.flush();
        std::cout << "Flushed 2 fields on 2 databases" << std::endl;

        // check FDB is populated

        EXPECT_EQUAL(countAll(fdb, {commonReq}), 2);
        std::cout << "Listed 2 fields" << std::endl;

        // wipe data

        WipeElement elem;
        std::map<WipeElementType, size_t> element_counts;

        // dry run attempt to wipe with too specific request

        auto wipeObject = fdb.wipe(fullReq1);
        while (wipeObject.next(elem)) {
            element_counts[elem.type()] += elem.uris().size();
        }
        EXPECT_EQUAL(element_counts[WipeElementType::STORE], 0);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE], 0);


        // dry run wipe index and data files
        wipeObject = fdb.wipe(indexReq1);
        element_counts.clear();
        while (wipeObject.next(elem)) {
            element_counts[elem.type()] += elem.uris().size();
        }

        EXPECT_EQUAL(element_counts[WipeElementType::STORE], 1);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_INDEX], 1);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE], 2);

        // dry run wipe all databases
        wipeObject = fdb.wipe(commonReq);
        element_counts.clear();
        while (wipeObject.next(elem)) {
            element_counts[elem.type()] += elem.uris().size();

        }
        EXPECT_EQUAL(element_counts[WipeElementType::STORE], 2);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_INDEX], 2);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE], 4);

        // ensure fields still exist
        EXPECT_EQUAL(countAll(fdb, {commonReq}), 2);
        std::cout << "Listed 2 fields" << std::endl;

        // attempt to wipe with too specific request, no dry run
        wipeObject = fdb.wipe(fullReq1, true);
        element_counts.clear();
        while (wipeObject.next(elem)) {
            element_counts[elem.type()] += elem.uris().size();
        }

        EXPECT_EQUAL(element_counts[WipeElementType::STORE], 0);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE], 0);
        
        // ensure fields still exist        
        EXPECT_EQUAL(countAll(fdb, {commonReq}), 2);
        std::cout << "Listed 2 fields" << std::endl;

        // wipe both databases, no dry run
        wipeObject = fdb.wipe(commonReq, true);
        element_counts.clear();
        while (wipeObject.next(elem)) {
            element_counts[elem.type()] += elem.uris().size();
        }
        EXPECT_EQUAL(element_counts[WipeElementType::STORE], 2);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_INDEX], 2);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE], 4);

        EXPECT_EQUAL(countAll(fdb, {commonReq}), 0);
        std::cout << "Wiped 2 databases" << std::endl;

        // check database directories do not exist
        if (configName == "localSingleRoot") {
            std::vector<eckit::PathName> dbFiles;
            std::vector<eckit::PathName> dbDirs;
            wipe_tests_tmp_root().children(dbFiles, dbDirs);
            ASSERT(dbFiles.size() == 0);
            ASSERT(dbDirs.size() == 0);
        }
        else if (configName == "localSeparateRoots") {
            std::vector<eckit::PathName> dbFiles;
            std::vector<eckit::PathName> dbDirs;
            wipe_tests_tmp_root().children(dbFiles, dbDirs);
            ASSERT(dbFiles.size() == 0);
            ASSERT(dbDirs.size() == 0);
            dbFiles.clear();
            dbDirs.clear();
            wipe_tests_tmp_root_store().children(dbFiles, dbDirs);
            ASSERT(dbFiles.size() == 0);
            ASSERT(dbDirs.size() == 0);
        }
    }

    /// @todo: if doing what's in this section at the end of the previous section reusing the same FDB object,
    // archive() fails as it expects a toc file to exist, but it has been removed by previous wipe
    SECTION("WIPE SINGLE DATABASE AND SINGLE INDEX") {

        // initialise FDB

        FDB fdb(config);

        // check FDB is empty

        EXPECT_EQUAL(countAll(fdb, {commonReq}), 0);
        std::cout << "Listed 0 fields" << std::endl;

        // rearchive both databases, archive req3 as well
        fdb.archive(requestKey1, data, sizeof(data));
        fdb.archive(requestKey2, data, sizeof(data));
        fdb.archive(requestKey3, data, sizeof(data));
        std::cout << "Archived 3 fields on 2 databases" << std::endl;

        fdb.flush();
        std::cout << "Flushed 3 fields on 2 databases" << std::endl;

        // check FDB is populated

        EXPECT_EQUAL(countAll(fdb, {commonReq}), 3);
        std::cout << "Listed 3 fields" << std::endl;

        // wipe one database
        WipeElement elem;
        auto wipeObject = fdb.wipe(dbReq1, true);
        while (wipeObject.next(elem)) {
            std::cout << elem << std::endl;
        }
        EXPECT_EQUAL(countAll(fdb, {commonReq}), 2);
        EXPECT_EQUAL(countAll(fdb, {dbReq1}), 0);
        std::cout << "Wiped 1 database" << std::endl;
        // check database1 directory does not exist
        std::vector<eckit::PathName> dbFiles;
        std::vector<eckit::PathName> dbDirs;
        if (configName == "localSingleRoot") {
            wipe_tests_tmp_root().children(dbFiles, dbDirs);
            ASSERT(dbFiles.size() == 0);
            ASSERT(dbDirs.size() == 1);
        }
        else if (configName == "localSeparateRoots") {
            wipe_tests_tmp_root().children(dbFiles, dbDirs);
            ASSERT(dbFiles.size() == 0);
            ASSERT(dbDirs.size() == 1);
            dbFiles.clear();
            dbDirs.clear();
            wipe_tests_tmp_root_store().children(dbFiles, dbDirs);
            ASSERT(dbFiles.size() == 0);
            ASSERT(dbDirs.size() == 1);
        }

        // wipe one index for other database
        wipeObject = fdb.wipe(indexReq2, true);
        while (wipeObject.next(elem)) {
            std::cout << elem << std::endl;
        }
        EXPECT_EQUAL(countAll(fdb, {commonReq}), 1);
        EXPECT_EQUAL(countAll(fdb, {indexReq2}), 0);
        std::cout << "Wiped 1 index" << std::endl;
        // check database2 only contains 4 files (toc, schema, index, data)
        if (configName == "localSingleRoot") {
            dbFiles.clear();
            dbDirs.clear();
            (wipe_tests_tmp_root() / dbKey2.valuesToString()).children(dbFiles, dbDirs);
            ASSERT(dbFiles.size() == 4);
            ASSERT(dbDirs.size() == 0);
        }
        else if (configName == "localSeparateRoots") {
            dbFiles.clear();
            dbDirs.clear();
            (wipe_tests_tmp_root() / dbKey2.valuesToString()).children(dbFiles, dbDirs);
            ASSERT(dbFiles.size() == 3);
            ASSERT(dbDirs.size() == 0);
            dbFiles.clear();
            dbDirs.clear();
            (wipe_tests_tmp_root_store() / dbKey2.valuesToString()).children(dbFiles, dbDirs);
            ASSERT(dbFiles.size() == 1);
            ASSERT(dbDirs.size() == 0);
        }

        // fully wipe remaning part of the database
        wipeObject = fdb.wipe(commonReq, true);
        while (wipeObject.next(elem)) {
            std::cout << elem << std::endl;
        }
        EXPECT_EQUAL(countAll(fdb, {commonReq}), 0);
    }

    /// @todo: if doing what's in this section at the end of the previous section reusing the same FDB object,
    // archive() fails as it expects a toc file to exist, but it has been removed by previous wipe
    SECTION("WIPE MASKED DATA") {

        // initialise FDB

        FDB fdb(config);

        // check FDB is empty

        EXPECT_EQUAL(countAll(fdb, {commonReq}), 0);
        std::cout << "Listed 0 fields" << std::endl;

        // rearchive second database (two indices) two times

        fdb.archive(requestKey2, data, sizeof(data));
        fdb.archive(requestKey3, data, sizeof(data));

        fdb.flush();
        std::cout << "Flushed 2 fields in 1 database" << std::endl;

        fdb.archive(requestKey2, data, sizeof(data));
        fdb.archive(requestKey3, data, sizeof(data));

        fdb.flush();
        std::cout << "Flushed 2 fields in 1 database" << std::endl;

        std::cout << "Archived 2 fields (4 including masked) in 1 database" << std::endl;

        // list masked and ensure there are four fields
        EXPECT_EQUAL(countAll(fdb, {commonReq}), 4);
        std::cout << "Listed 4 fields including masked" << std::endl;

        // list non-masked and ensure there are two fields
        EXPECT_EQUAL(countAll(fdb, {commonReq}, true), 2);
        std::cout << "Listed 2 fields excluding masked" << std::endl;

        // wipe one index and ensure its field is gone
        WipeElement elem;
        auto wipeObject = fdb.wipe(indexReq2, true);
        std::map<WipeElementType, size_t> element_counts;
        while (wipeObject.next(elem)) {
            element_counts[elem.type()] += elem.uris().size();
        }

        EXPECT_EQUAL(element_counts[WipeElementType::STORE], 1); // 1 data file
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_INDEX], 1); // 1 index
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE], 0); // no tocs or schemas wiped
        EXPECT_EQUAL(element_counts[WipeElementType::STORE_SAFE], 1); // 1 data
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_SAFE], 6); // 1 index, schema, toc and the 3 locks

        EXPECT_EQUAL(countAll(fdb, {commonReq}), 2);
        EXPECT_EQUAL(countAll(fdb, {indexReq2}), 0);
        EXPECT_EQUAL(countAll(fdb, {indexReq3}), 2);
        EXPECT_EQUAL(countAll(fdb, {indexReq3}, true), 1);
        std::cout << "Wiped 1 index" << std::endl;
        // check database2 only contains 4 files (toc, schema, index, data)
        std::vector<eckit::PathName> dbFiles;
        std::vector<eckit::PathName> dbDirs;
        if (configName == "localSingleRoot") {
            (wipe_tests_tmp_root() / dbKey2.valuesToString()).children(dbFiles, dbDirs);
            ASSERT(dbFiles.size() == 4);
            ASSERT(dbDirs.size() == 0);
        }
        else if (configName == "localSeparateRoots") {
            (wipe_tests_tmp_root() / dbKey2.valuesToString()).children(dbFiles, dbDirs);
            ASSERT(dbFiles.size() == 3);
            ASSERT(dbDirs.size() == 0);
            dbFiles.clear();
            dbDirs.clear();
            (wipe_tests_tmp_root_store() / dbKey2.valuesToString()).children(dbFiles, dbDirs);
            ASSERT(dbFiles.size() == 1);
            ASSERT(dbDirs.size() == 0);
        }


        // wipe all database and ensure no fields can be listed, and all dirs are gone
        wipeObject = fdb.wipe(dbReq2, true);
        element_counts.clear();
        while (wipeObject.next(elem)) {
            element_counts[elem.type()] += elem.uris().size();
        }
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_SAFE], 0);
        EXPECT_EQUAL(element_counts[WipeElementType::STORE_SAFE], 0);
        EXPECT_EQUAL(element_counts[WipeElementType::STORE], 1);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_INDEX], 1);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE], 2);

        EXPECT_EQUAL(countAll(fdb, {dbReq2}), 0);
        std::cout << "Wiped database" << std::endl;
        // check database directories do not exist
        if (configName == "localSingleRoot") {
            dbFiles.clear();
            dbDirs.clear();
            wipe_tests_tmp_root().children(dbFiles, dbDirs);
            ASSERT(dbFiles.size() == 0);
            ASSERT(dbDirs.size() == 0);
        }
        else if (configName == "localSeparateRoots") {
            dbFiles.clear();
            dbDirs.clear();
            wipe_tests_tmp_root().children(dbFiles, dbDirs);
            ASSERT(dbFiles.size() == 0);
            ASSERT(dbDirs.size() == 0);
            dbFiles.clear();
            dbDirs.clear();
            (wipe_tests_tmp_root_store() / dbKey2.valuesToString()).children(dbFiles, dbDirs);
            ASSERT(dbFiles.size() == 0);
            ASSERT(dbDirs.size() == 0);
        }
    }
}

}  // namespace test
}  // namespace fdb5

int main(int argc, char** argv) {
    int failures = 0;

    // FDB configurations to test

    std::string localSingleRootConfig = R"(
spaces:
- roots:
  - path: )" + wipe_tests_tmp_root().asString() +
                                        R"(
type: local
schema : )" + schema_file().path() +
                                        R"(
engine: toc
store: file)";

    std::string localSeparateRootsConfig = R"(
spaces:
- catalogueRoots:
  - path: )" + wipe_tests_tmp_root().asString() +
                                           R"(
  storeRoots:
  - path: )" + wipe_tests_tmp_root_store().asString() +
                                           R"(
type: local
schema : )" + schema_file().path() +
                                           R"(
engine: toc
store: file)";

    /// @todo:

    // std::string localCatalogueRemoteStoreConfig{};

    // std::string remoteCatalogueLocalStoreConfig{};

    // std::string remoteSingleServerConfig{};

    // std::string remoteSeparateServersConfig{};

    std::map<std::string, std::string> configurations{
        {"localSingleRoot", localSingleRootConfig},
        // {"localSeparateRoots", localSeparateRootsConfig},
        // {"localCatalogueRemoteStore", localCatalogueRemoteStoreConfig},
        // ...
    };

    for (const auto& configEntry : configurations) {

        configName = configEntry.first;
        config     = fdb5::Config{YAMLConfiguration(configEntry.second)};

        std::cout << std::endl;
        std::cout << "---------------------------------" << std::endl;
        std::cout << "TESTING CONFIG " << configName << std::endl;
        std::cout << "---------------------------------" << std::endl;
        std::cout << std::endl;

        failures += run_tests(argc, argv);
    }

    return failures;
}
