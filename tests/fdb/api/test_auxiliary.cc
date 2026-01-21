#include <string>
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/TmpDir.h"
#include "eckit/testing/Test.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/toc/TocCommon.h"

namespace fdb5::test {

//----------------------------------------------------------------------------------------------------------------------

eckit::PathName writeAuxiliaryData(const eckit::PathName datapath, const std::string ext) {
    eckit::PathName auxpath(datapath + "." + ext);
    std::string data_str = "Some extra data";
    const void* data     = static_cast<const void*>(data_str.c_str());
    size_t length        = data_str.size();
    eckit::FileHandle file(auxpath);
    file.openForWrite(0);
    file.write(data, length);
    file.close();
    return auxpath;
}

std::set<eckit::PathName> setup(FDB& fdb, std::set<std::string> auxExtensions = {"foo", "bar"},
                                const std::vector<std::string>& dates = {"20101010", "20111213"},
                                const std::vector<std::string>& types = {"fc", "pf"},
                                const std::vector<std::string>& steps = {"1", "2"}

) {
    // Setup: Write data, generating auxiliary files using the archive callback
    std::set<eckit::PathName> auxPaths;
    fdb.registerArchiveCallback([&auxPaths, auxExtensions](const Key& key, const void* data, size_t length,
                                                           std::future<std::shared_ptr<const FieldLocation>> future) {
        std::shared_ptr<const FieldLocation> location = future.get();
        for (const auto& ext : auxExtensions) {
            auxPaths.insert(writeAuxiliaryData(location->uri().path(), ext));
        }
    });

    std::string data_str = "Raining cats and dogs";
    const void* data     = static_cast<const void*>(data_str.c_str());
    size_t length        = data_str.size();

    Key key;
    key.set("class", "od");
    key.set("expver", "xxxx");
    key.set("type", "fc");
    key.set("stream", "oper");
    key.set("date", "20101010");
    key.set("time", "0000");
    key.set("domain", "g");
    key.set("levtype", "sfc");
    key.set("param", "130");

    for (const auto& date : dates) {
        key.set("date", date);
        for (const auto& type : types) {
            key.set("type", type);
            for (const auto& step : steps) {
                key.set("step", step);
                fdb.archive(key, data, length);
            }
        }
    }

    fdb.flush();

    return auxPaths;
}

void expectEmpty(eckit::PathName dir) {

    // If this directory exists, the wipe did not complete. Print some useful diagnostic info...
    if (dir.exists()) {
        eckit::Log::error() << "err -- expected dir to no longer exist: " << dir << std::endl;

        std::vector<eckit::PathName> allPaths;
        StdDir(dir).children(allPaths);

        if (allPaths.empty()) {
            eckit::Log::error() << "err -- (directory is empty)" << std::endl;
        }

        for (const eckit::PathName& path : allPaths) {
            eckit::Log::error() << "err -- unwiped path: " << path << std::endl;
        }
    }

    EXPECT_NOT(dir.exists());
}

//----------------------------------------------------------------------------------------------------------------------

CASE("Wipe with extensions") {
    eckit::TmpDir tmpdir(eckit::LocalPathName::cwd().c_str());
    eckit::testing::SetEnv env_config{"FDB_ROOT_DIRECTORY", tmpdir.asString().c_str()};

    eckit::testing::SetEnv env_extensions{"FDB_AUX_EXTENSIONS", "foo,bar"};
    FDB fdb;
    std::set<eckit::PathName> auxPaths = setup(fdb);
    EXPECT_EQUAL(auxPaths.size(), 8);
    for (const auto& auxPath : auxPaths) {
        EXPECT(auxPath.exists());
    }

    // call wipe, without doit
    FDBToolRequest request = FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0];
    bool doit              = false;
    auto iter              = fdb.wipe(request, doit);

    WipeElement elem;
    std::map<WipeElementType, size_t> element_counts;
    while (iter.next(elem)) {
        element_counts[elem.type()] += elem.uris().size();
    }
    // 4 .index files, 4 .data files and 8 aux files
    // 2 dbs, each with 1 schema and 1 toc --> 4 CATALOGUE files
    EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE], 4);
    EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_INDEX], 4);
    EXPECT_EQUAL(element_counts[WipeElementType::STORE], 4);
    EXPECT_EQUAL(element_counts[WipeElementType::STORE_AUX], 8);

    // over specified wipe: returns nothing
    request = FDBToolRequest::requestsFromString("class=od,expver=xxxx,type=pf,step=1")[0];
    doit    = true;
    iter    = fdb.wipe(request, doit);
    element_counts.clear();
    while (iter.next(elem)) {
        element_counts[elem.type()] += elem.uris().size();
    }
    EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE], 0);  // no DBs are fully wiped
    EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_INDEX], 0);
    EXPECT_EQUAL(element_counts[WipeElementType::STORE], 0);
    EXPECT_EQUAL(element_counts[WipeElementType::STORE_AUX], 0);


    // partial wipe on the second level. Hits half the data files
    request = FDBToolRequest::requestsFromString("class=od,expver=xxxx,type=pf")[0];
    doit    = true;
    iter    = fdb.wipe(request, doit);
    element_counts.clear();
    while (iter.next(elem)) {
        element_counts[elem.type()] += elem.uris().size();
    }
    EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE], 0);  // no DBs are fully wiped
    EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_INDEX], 2);
    EXPECT_EQUAL(element_counts[WipeElementType::STORE], 2);
    EXPECT_EQUAL(element_counts[WipeElementType::STORE_AUX], 4);

    // call wipe on everything which remains
    request = FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0];
    doit    = true;
    iter    = fdb.wipe(request, doit);
    element_counts.clear();
    while (iter.next(elem)) {
        element_counts[elem.type()] += elem.uris().size();
    }
    EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE], 4);
    EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_INDEX], 2);
    EXPECT_EQUAL(element_counts[WipeElementType::STORE], 2);
    EXPECT_EQUAL(element_counts[WipeElementType::STORE_AUX], 4);

    // Check that the auxiliary files have been removed
    for (const auto& auxPath : auxPaths) {
        EXPECT(!auxPath.exists());
    }

    // Check that all data and index files are gone, and the dir deleted.
    expectEmpty(auxPaths.begin()->dirName());
}

CASE("Ensure wipe fails if extensions are unknown") {
    eckit::TmpDir tmpdir(eckit::LocalPathName::cwd().c_str());
    eckit::testing::SetEnv env_config{"FDB_ROOT_DIRECTORY", tmpdir.asString().c_str()};

    eckit::testing::SetEnv env_extensions{"FDB_AUX_EXTENSIONS", "foo,bar"};
    FDB fdb;
    std::set<eckit::PathName> auxPaths =
        setup(fdb, {"foo", "UNKNOWN"});  // Write ".UNKNOWN" files, which should prevent wipe.
    EXPECT_EQUAL(auxPaths.size(), 8);
    for (const auto& auxPath : auxPaths) {
        EXPECT(auxPath.exists());
    }

    // call wipe
    FDBToolRequest request = FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0];
    bool doit              = true;
    bool unsafeWipeAll     = false;
    bool error_was_thrown  = false;
    WipeElement elem;

    // Expect an error to be thrown when unsafeWipeAll is false.
    try {
        auto iter = fdb.wipe(request, doit, false, unsafeWipeAll);
        while (iter.next(elem)) {}
    }
    catch (eckit::Exception) {
        error_was_thrown = true;
    }
    EXPECT(error_was_thrown);

    // Check that the auxiliary files all still exist.
    for (const auto& auxPath : auxPaths) {
        EXPECT(auxPath.exists());
    }

    // Check we can still list the data (i.e. there isn't a lingering lock)
    auto listObject = fdb.list(request, false);
    size_t count    = 0;
    ListElement le;
    while (listObject.next(le)) {
        eckit::Log::info() << le;
        count++;
    }
    EXPECT(count > 0);

    // Use unsafe wipe all to actually delete everything.
    unsafeWipeAll = true;
    auto iter     = fdb.wipe(request, doit, false, unsafeWipeAll);

    std::map<WipeElementType, size_t> element_counts;
    while (iter.next(elem)) {
        eckit::Log::info() << elem;
        element_counts[elem.type()] += elem.uris().size();
    }
    EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE], 4);
    EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_INDEX], 4);
    EXPECT_EQUAL(element_counts[WipeElementType::STORE], 4);
    EXPECT_EQUAL(element_counts[WipeElementType::STORE_AUX], 4);
    EXPECT_EQUAL(element_counts[WipeElementType::UNKNOWN], 4);  // The .UNKNOWN aux files

    // Check that the auxiliary files have been removed
    for (const auto& auxPath : auxPaths) {
        EXPECT(!auxPath.exists());
    }

    // Check that all data and index files are gone, and the dir deleted.
    expectEmpty(auxPaths.begin()->dirName());
}

CASE("Purge with extensions") {
    eckit::TmpDir tmpdir(eckit::LocalPathName::cwd().c_str());
    eckit::testing::SetEnv env_config{"FDB_ROOT_DIRECTORY", tmpdir.asString().c_str()};

    eckit::testing::SetEnv env_extensions{"FDB_AUX_EXTENSIONS", "foo,bar"};
    std::set<eckit::PathName> auxPathsDelete;

    // Archive the same data three times
    for (int i = 0; i < 2; i++) {
        FDB fdb;
        auto aux = setup(fdb);
        auxPathsDelete.insert(aux.begin(), aux.end());
    }
    FDB fdb;
    auto auxPathsKeep = setup(fdb);

    EXPECT_EQUAL(auxPathsDelete.size(), 16);
    EXPECT_EQUAL(auxPathsKeep.size(), 8);

    for (const auto& auxPath : auxPathsDelete) {
        EXPECT(auxPath.exists());
    }
    for (const auto& auxPath : auxPathsKeep) {
        EXPECT(auxPath.exists());
    }

    // call purge
    FDBToolRequest request = FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0];
    bool doit              = true;
    auto listObject        = fdb.purge(request, doit, false);

    PurgeElement elem;
    while (listObject.next(elem)) {
        eckit::Log::info() << elem << std::endl;
    }

    // Check that the masked auxiliary files have been removed
    for (const auto& auxPath : auxPathsDelete) {
        EXPECT(!auxPath.exists());
    }

    // Check that the unmasked auxiliary files have not been removed
    for (const auto& auxPath : auxPathsKeep) {
        EXPECT(auxPath.exists());
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::test

int main(int argc, char** argv) {

    eckit::Log::info() << ::getenv("FDB_HOME") << std::endl;

    return ::eckit::testing::run_tests(argc, argv);
}
