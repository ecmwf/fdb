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
    std::vector<Key> internalKeys;

    fdb.registerCallback([&map] (const fdb5::Key& internalKey, const fdb5::FieldLocation& location) {
        map[internalKey] = location.fullUri();
    });

	key.set("step","1");
    internalKeys.push_back(fdb.archive(key, data, length));

	key.set("step","2");
    internalKeys.push_back(fdb.archive(key, data, length));

	key.set("step","3");
    internalKeys.push_back(fdb.archive(key, data, length));
    
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
            if (key == internalKey){
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
