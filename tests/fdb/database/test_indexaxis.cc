

#include "fdb5/database/IndexAxis.h"
#include "fdb5/database/Key.h"

#include "eckit/io/Buffer.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/serialisation/ResizableMemoryStream.h"
#include "eckit/testing/Test.h"

namespace {

fdb5::Key EXAMPLE_K1{{
    {"class", "od"},
    {"expver", "0001"},
    {"date", "20240223"}
}};

fdb5::Key EXAMPLE_K2{{
    {"class", "rd"},
     {"expver", "0001"},
     {"time", "1200"}
}};

fdb5::Key EXAMPLE_K3{{
    {"class", "rd"},
    {"expver", "gotx"},
    {"time", "0000"}
}};

//----------------------------------------------------------------------------------------------------------------------

CASE("Insertion and comparison") {

    fdb5::IndexAxis ia1;
    fdb5::IndexAxis ia2;
    EXPECT(ia1 == ia2);
    EXPECT(!(ia1 != ia2));

    ia1.insert(EXAMPLE_K1);
    EXPECT(!(ia1 == ia2));
    EXPECT(ia1 != ia2);

    ia2.insert(EXAMPLE_K2);
    EXPECT(!(ia1 == ia2));
    EXPECT(ia1 != ia2);

    ia1.insert(EXAMPLE_K2);
    EXPECT(!(ia1 == ia2));
    EXPECT(ia1 != ia2);

    ia2.insert(EXAMPLE_K1);

    EXPECT(!(ia1 == ia2));
    EXPECT(ia1 != ia2);
    ia1.sort();
    ia2.sort();
    EXPECT(ia1 == ia2);
    EXPECT(!(ia1 != ia2));
}

CASE("iostream and JSON output functions correctly") {

    fdb5::IndexAxis ia;

    {
        std::ostringstream ss;
        ss << ia;
        EXPECT(ss.str() == "IndexAxis[axis={}]");
    }

    {
        std::ostringstream ss;
        eckit::JSON json(ss);
        json << ia;
        EXPECT(ss.str() == "{}");
    }

    ia.insert(EXAMPLE_K1);
    ia.insert(EXAMPLE_K2);
    ia.insert(EXAMPLE_K3);
    ia.sort();

    {
        std::ostringstream ss;
        ss << ia;
        EXPECT(ss.str() == "IndexAxis[axis={class=(od,rd),date=(20240223),expver=(0001,gotx),time=(0000,1200)}]");
    }

    {
        std::ostringstream ss;
        eckit::JSON json(ss);
        json << ia;
        EXPECT(ss.str() == "{\"class\":[\"od\",\"rd\"],\"date\":[\"20240223\"],\"expver\":[\"0001\",\"gotx\"],\"time\":[\"0000\",\"1200\"]}");
    }
}

CASE("serialiastion and deserialisation") {

    fdb5::IndexAxis ia;
    ia.insert(EXAMPLE_K1);
    ia.insert(EXAMPLE_K2);
    ia.insert(EXAMPLE_K3);
    ia.sort();

    eckit::Buffer buf;
    {
        eckit::ResizableMemoryStream ms(buf);
        ia.encode(ms, fdb5::IndexAxis::currentVersion());
    }

    {
        eckit::MemoryStream ms(buf);
        fdb5::IndexAxis newia(ms, fdb5::IndexAxis::currentVersion());
        EXPECT(ia == newia);
    }
}

CASE("Check that merging works correctly") {

    fdb5::IndexAxis ia1;
    ia1.insert(EXAMPLE_K1);
    ia1.insert(EXAMPLE_K2);
    ia1.sort();

    fdb5::IndexAxis ia2;
    ia2.insert(EXAMPLE_K1);
    ia2.insert(EXAMPLE_K3);
    ia2.sort();

    EXPECT(ia1 != ia2);

    fdb5::IndexAxis iatest;
    iatest.insert(EXAMPLE_K1);
    iatest.insert(EXAMPLE_K2);
    iatest.insert(EXAMPLE_K3);
    iatest.sort();

    EXPECT(iatest != ia1);
    EXPECT(iatest != ia2);

    ia1.merge(ia2);

    EXPECT(iatest == ia1);
    EXPECT(iatest != ia2);
}

CASE("Copy internal map") {

    fdb5::IndexAxis ia;
    ia.insert(EXAMPLE_K1);
    ia.insert(EXAMPLE_K2);
    ia.insert(EXAMPLE_K3);
    ia.sort();

    std::map<std::string, eckit::DenseSet<std::string>> map = ia.map();

    EXPECT(map.size() == 4);
    for (const auto& [k, v] : map) {
        EXPECT(v == ia.values(k));
    }

    // Make sure it's not a shallow copy
    map["class"].insert("new1");
    map["class"].sort();
    EXPECT(map["class"].contains("new1"));
    EXPECT(!ia.values("class").contains("new1"));

    ia.insert(fdb5::Key{{{"class", "new2"}}});
    ia.sort();
    EXPECT(ia.values("class").contains("new2"));
    EXPECT(!map["class"].contains("new2"));
}

//----------------------------------------------------------------------------------------------------------------------

} // anonymous namespace

int main(int argc, char** argv) {

    eckit::Log::info() << ::getenv("FDB_HOME") << std::endl;

    return ::eckit::testing::run_tests(argc, argv);
}
