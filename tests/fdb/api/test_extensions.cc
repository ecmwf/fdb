#include "eckit/testing/Test.h"
#include "metkit/mars/MarsRequest.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

namespace fdb5::test {

//----------------------------------------------------------------------------------------------------------------------

std::vector<std::string> extensions = {"foo", "bar"};

eckit::PathName writeAuxiliaryData(const eckit::PathName datapath, const std::string ext) {
    eckit::PathName auxpath(datapath + "." + ext);
    std::string data_str = "Some extra data";
    const void* data = static_cast<const void *>(data_str.c_str());
    size_t length = data_str.size();
    eckit::FileHandle file(auxpath);
    file.openForWrite(0);
    file.write(data, length);
    file.close();
    return auxpath;
}

void setup(FDB& fdb) {
    // Setup: Write data, generating auxiliary files using the archive callback
    fdb.registerArchiveCallback([] (const Key& key, const void* data, size_t length, std::future<std::shared_ptr<FieldLocation>> future) {
        std::shared_ptr<FieldLocation> location = future.get();
        for (const auto& ext : extensions) {
            writeAuxiliaryData(location->uri().path(), ext);
        }
    });

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

    key.set("step","1");
    fdb.archive(key, data, length);

    key.set("date","20111213");
    fdb.archive(key, data, length);

    key.set("type","pf");
    fdb.archive(key, data, length);
 
    fdb.flush();

}

//----------------------------------------------------------------------------------------------------------------------

CASE("Wipe with extensions") {

    ::setenv("FDB_AUX_EXTENSIONS", "foo,bar", 1);

    FDB fdb;
    setup(fdb);
    
    // call wipe
    FDBToolRequest request = FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0];
    bool doit = true;
    auto listObject = fdb.wipe(request, doit);

    WipeElement elem;
    while (listObject.next(elem)) {
        eckit::Log::info() << elem << std::endl;
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5::test

int main(int argc, char** argv) {

    eckit::Log::info() << ::getenv("FDB_HOME") << std::endl;

    return ::eckit::testing::run_tests(argc, argv);
}
