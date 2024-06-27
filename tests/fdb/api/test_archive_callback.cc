#include "eckit/testing/Test.h"
#include "fdb5/api/FDB.h"

namespace fdb5::test {

//----------------------------------------------------------------------------------------------------------------------
CASE("Archive callback") {
    FDB fdb;

    std::string data_str = "Raining cats and dogs";
    const void* data = static_cast<const void *>(data_str.c_str());
    size_t length = data_str.size();

    Key key;
    key.set("class","od");
    key.set("expver","xxxx");
    key.set("type","fc");
    key.set("stream","oper");
    key.set("date","20101010");
    key.set("time","0000");
    key.set("domain","g");
    key.set("levtype","sfc");
    key.set("param","130");

    std::map<fdb5::Key, eckit::URI> map;
    std::vector<Key> keys;

    fdb.registerCallback([&map] (const fdb5::Key& key, const fdb5::FieldLocation& location) {
        map[key] = location.fullUri();
    });

    key.set("step","1");
    keys.push_back(key);
    fdb.archive(key, data, length);

    key.set("date","20111213");
    keys.push_back(key);
    fdb.archive(key, data, length);

    key.set("type","pf");
    keys.push_back(key);
    fdb.archive(key, data, length);
    
    fdb.flush();

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
