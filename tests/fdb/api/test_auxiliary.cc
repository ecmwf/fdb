#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/TmpDir.h"
#include "eckit/testing/Test.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "metkit/mars/MarsRequest.h"

namespace fdb5::test {

//----------------------------------------------------------------------------------------------------------------------

std::set<std::string> extensions = {"foo", "bar"};

eckit::PathName writeAuxiliaryData(const eckit::PathName datapath, const std::string ext) {
    eckit::PathName auxpath(datapath + "." + ext);
    std::string data_str = "Some extra data";
    const void* data = static_cast<const void*>(data_str.c_str());
    size_t length = data_str.size();
    eckit::FileHandle file(auxpath);
    file.openForWrite(0);
    file.write(data, length);
    file.close();
    return auxpath;
}

std::set<eckit::PathName> setup(FDB& fdb) {
    // Setup: Write data, generating auxiliary files using the archive callback
    std::set<eckit::PathName> auxPaths;
    fdb.registerArchiveCallback([&auxPaths](const Key& key, const void* data, size_t length,
                                            std::future<std::shared_ptr<const FieldLocation>> future) {
        std::shared_ptr<const FieldLocation> location = future.get();
        for (const auto& ext : extensions) {
            auxPaths.insert(writeAuxiliaryData(location->uri().path(), ext));
        }
    });

    std::string data_str = "Raining cats and dogs";
    const void* data = static_cast<const void*>(data_str.c_str());
    size_t length = data_str.size();

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

    key.set("step", "1");
    fdb.archive(key, data, length);

    key.set("date", "20111213");
    fdb.archive(key, data, length);

    key.set("type", "pf");
    fdb.archive(key, data, length);

    fdb.flush();

    return auxPaths;
}

//----------------------------------------------------------------------------------------------------------------------

CASE("Wipe with extensions") {
    eckit::TmpDir tmpdir(eckit::LocalPathName::cwd().c_str());
    eckit::testing::SetEnv env_config{"FDB_ROOT_DIRECTORY", tmpdir.asString().c_str()};

    eckit::testing::SetEnv env_extensions{"FDB_AUX_EXTENSIONS", "foo,bar"};
    FDB fdb;
    std::set<eckit::PathName> auxPaths = setup(fdb);
    EXPECT(auxPaths.size() == 6);
    for (const auto& auxPath : auxPaths) {
        EXPECT(auxPath.exists());
    }

    // call wipe
    FDBToolRequest request = FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0];
    bool doit = true;
    auto listObject = fdb.wipe(request, doit);

    WipeElement elem;
    while (listObject.next(elem)) {
        eckit::Log::info() << elem << std::endl;
    }

    // Check that the auxiliary files have been removed
    for (const auto& auxPath : auxPaths) {
        EXPECT(!auxPath.exists());
    }
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

    EXPECT(auxPathsDelete.size() == 12);
    EXPECT(auxPathsKeep.size() == 6);

    for (const auto& auxPath : auxPathsDelete) {
        EXPECT(auxPath.exists());
    }
    for (const auto& auxPath : auxPathsKeep) {
        EXPECT(auxPath.exists());
    }

    // call purge
    FDBToolRequest request = FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0];
    bool doit = true;
    auto listObject = fdb.purge(request, doit, false);

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
