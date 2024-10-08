#include "eckit/testing/Test.h"
#include "fdb5/api/FDB.h"

namespace fdb5::test {

//----------------------------------------------------------------------------------------------------------------------
CASE("Archive and flush callback") {
    FDB fdb;

    std::string data_str = "Raining cats and dogs";
    const void* data = static_cast<const void *>(data_str.c_str());
    size_t length = data_str.size();

    Key key;
    key.push("class","od");
    key.push("expver","xxxx");
    key.push("type","fc");
    key.push("stream","oper");
    key.push("date","20101010");
    key.push("time","0000");
    key.push("domain","g");
    key.push("levtype","sfc");
    key.push("param","130");

    std::map<fdb5::Key, eckit::URI> map;
    std::vector<Key> keys;
    bool flushCalled = false;

    fdb.registerArchiveCallback([&map] (const Key& key, const void* data, size_t length, std::future<std::shared_ptr<const FieldLocation>> future) {
        std::shared_ptr<const FieldLocation> location = future.get();
        map[key] = location->fullUri();
    });

    fdb.registerFlushCallback([&flushCalled] () {
        flushCalled = true;
    });

    key.push("step","1");
    keys.push_back(key);
    fdb.archive(key, data, length);

    key.push("date","20111213");
    keys.push_back(key);
    fdb.archive(key, data, length);

    key.push("type","pf");
    keys.push_back(key);
    fdb.archive(key, data, length);
    
    fdb.flush();

    EXPECT(flushCalled);

    EXPECT(map.size() == 3);

    // for (const auto& [key, uri] : map) {
    //     std::cout << key << " -> " << uri << std::endl;
    // }

    for (const auto& [key, uri] : map) {
        bool found = false;
        for (const auto& originalKey : keys) {
            if (key == originalKey){
                found = true;
                break;
            }
        }
        EXPECT(found);
    }
    
}
//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5::test

int main(int argc, char** argv) {

    eckit::Log::info() << ::getenv("FDB_HOME") << std::endl;

    return ::eckit::testing::run_tests(argc, argv);
}
