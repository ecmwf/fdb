

#include "eckit/testing/Test.h"
#include "fdb5/api/FDB.h"

namespace {
bool testEqual(const fdb5::Key& key1, const fdb5::Key& key2) {
    if (key1.size() != key2.size()) {
        std::cout << "key1.size() != key2.size()" << std::endl;
        return false;
    }

    // Then check that all items in key1 are in key2
    for (const auto& item : key1) {
        if (key2.find(item.first) == key2.end()) {
            return false;
        }

        if (key2.value(item.first) != item.second) {
            return false;
        }
    }

    return true;
}
    
} // namespace anonymous


namespace fdb5::test {

//----------------------------------------------------------------------------------------------------------------------
CASE("Archive callback 2") {
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
    std::vector<Key> internalKeys;

	key.set("step","1");
    internalKeys.push_back(fdb.archive(key, data, length, [&map, key] (const fdb5::Key& fullKey, const fdb5::FieldLocation& location) {
        map[key] = location.fullUri();
    }));

	key.set("step","2");
    internalKeys.push_back(fdb.archive(key, data, length, [&map, key] (const fdb5::Key& fullKey, const fdb5::FieldLocation& location) {
        map[key] = location.fullUri();
    }));

	key.set("step","3");
    internalKeys.push_back(fdb.archive(key, data, length, [&map, key] (const fdb5::Key& fullKey, const fdb5::FieldLocation& location) {
        map[key] = location.fullUri();
    }));
    
	fdb.flush();

    EXPECT(map.size() == 3);

    // Print out the map
    for (const auto& [key, uri] : map) {
        std::cout << key << " -> " << uri << std::endl;
    }

    // Note that the keys are not the same, but they are equivalent
    // so iterate over the map keys and check that there is a corresponding key in the internalKeys vector
    for (const auto& [key, uri] : map) {
        bool found = false;
        for (const auto& internalKey : internalKeys) {
            if (testEqual(key, internalKey)) {
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
