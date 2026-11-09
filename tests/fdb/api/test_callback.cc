#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/TmpDir.h"
#include "eckit/testing/Test.h"
#include "fdb5/api/FDB.h"

namespace fdb5::test {

//----------------------------------------------------------------------------------------------------------------------
CASE("Archive and flush callback") {

    eckit::TmpDir tmpdir(eckit::LocalPathName::cwd().c_str());
    eckit::testing::SetEnv env_config{"FDB_ROOT_DIRECTORY", tmpdir.asString().c_str()};

    FDB fdb;

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

    std::map<fdb5::Key, eckit::URI> map;
    std::vector<Key> keys;
    bool flushCalled = false;

    fdb.registerArchiveCallback([&map](const Key& key, const void* data, size_t length,
                                       std::future<std::shared_ptr<const FieldLocation>> future) {
        std::shared_ptr<const FieldLocation> location = future.get();
        map[key] = location->fullUri();
    });

    fdb.registerFlushCallback([&flushCalled]() { flushCalled = true; });

    key.set("step", "1");
    keys.push_back(key);
    fdb.archive(key, data, length);

    key.set("date", "20111213");
    keys.push_back(key);
    fdb.archive(key, data, length);

    key.set("type", "an");
    keys.push_back(key);
    fdb.archive(key, data, length);

    fdb.flush();

    EXPECT(flushCalled);

    EXPECT_EQUAL(map.size(), 3);

    for (const auto& [key, uri] : map) {
        bool found = false;
        for (const auto& originalKey : keys) {
            if (key == originalKey) {
                found = true;
                break;
            }
        }
        EXPECT(found);
    }
}
//----------------------------------------------------------------------------------------------------------------------
CASE("Multiple archive callbacks retain independent futures and registration order") {

    eckit::TmpDir tmpdir(eckit::LocalPathName::cwd().c_str());
    eckit::testing::SetEnv env_config{"FDB_ROOT_DIRECTORY", tmpdir.asString().c_str()};

    FDB fdb;
    const std::string data = "Raining cats and dogs";
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
    key.set("step", "0");

    // Creating the archiver with no callbacks must not prevent later registration.
    fdb.archive(key, data.data(), data.size());

    std::vector<int> order;
    std::vector<std::shared_ptr<const FieldLocation>> locations;
    std::vector<std::future<std::shared_ptr<const FieldLocation>>> futures;
    size_t lateCalls = 0;

    fdb.registerArchiveCallback([&](const Key& archivedKey, const void* archivedData, size_t length,
                                    std::future<std::shared_ptr<const FieldLocation>> future) {
        EXPECT(archivedKey == key);
        EXPECT(archivedData == data.data());
        EXPECT_EQUAL(length, data.size());
        order.push_back(1);
        locations.push_back(future.get());
    });
    fdb.registerArchiveCallback([&](const Key& archivedKey, const void* archivedData, size_t length,
                                    std::future<std::shared_ptr<const FieldLocation>> future) {
        EXPECT(archivedKey == key);
        EXPECT(archivedData == data.data());
        EXPECT_EQUAL(length, data.size());
        order.push_back(2);
        // Retain the future after the callback returns, as asynchronous consumers do.
        futures.push_back(std::move(future));
    });

    key.set("step", "1");
    fdb.archive(key, data.data(), data.size());

    fdb.registerArchiveCallback([&](const Key& archivedKey, const void* archivedData, size_t length,
                                    std::future<std::shared_ptr<const FieldLocation>> future) {
        EXPECT(archivedKey == key);
        EXPECT(archivedData == data.data());
        EXPECT_EQUAL(length, data.size());
        order.push_back(3);
        EXPECT(future.get() == locations.back());
        ++lateCalls;
    });

    key.set("step", "2");
    fdb.archive(key, data.data(), data.size());
    key.set("step", "3");
    fdb.archive(key, data.data(), data.size());
    fdb.flush();

    const std::vector<int> expectedOrder{1, 2, 1, 2, 3, 1, 2, 3};
    EXPECT(order == expectedOrder);
    EXPECT_EQUAL(lateCalls, 2);
    EXPECT_EQUAL(locations.size(), 3);
    EXPECT_EQUAL(futures.size(), 3);
    for (size_t i = 0; i < futures.size(); ++i) {
        EXPECT(locations[i]);
        EXPECT(futures[i].get() == locations[i]);
    }
}
//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::test

int main(int argc, char** argv) {

    eckit::Log::info() << ::getenv("FDB_HOME") << std::endl;

    return ::eckit::testing::run_tests(argc, argv);
}
