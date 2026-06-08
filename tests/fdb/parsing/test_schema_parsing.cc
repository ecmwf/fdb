
#include "eckit/testing/Test.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/rules/SchemaParser.h"

namespace {

//----------------------------------------------------------------------------------------------------------------------

CASE("Broken schema - Non-existing schema file") {
    EXPECT_THROWS_AS(fdb5::Schema("./data/non-existing"), eckit::CantOpenFile);
    try {
        fdb5::Schema schema("./data/non-existing");
    }
    catch (eckit::Exception& ex) {
        std::cout << ex.what() << std::endl;
        EXPECT(std::string(ex.what()).find("Cannot open") != std::string::npos);
    }
}

CASE("Broken schema - No Rule") {
    EXPECT_THROWS(fdb5::Schema schema("./data/broken_schema_no_rule"));
    try {
        fdb5::Schema("./data/broken_schema_no_rule");
    }
    catch (fdb5::SchemaParser::Error& ex) {
        std::cout << ex.what() << std::endl;
        EXPECT(std::string(ex.what()).find("SchemaParser::parse: Empty rule list") != std::string::npos);
    }
}

CASE("Broken schema - No Rule but comments") {
    EXPECT_THROWS(fdb5::Schema schema("./data/broken_schema_comments_no_rule"));
    try {
        fdb5::Schema("./data/broken_schema_comments_no_rule");
    }
    catch (fdb5::SchemaParser::Error& ex) {
        std::cout << ex.what() << std::endl;
        EXPECT(std::string(ex.what()).find("SchemaParser::parse: Empty rule list") != std::string::npos);
    }
}

CASE("Broken schema - Missing semicolon types") {
    EXPECT_THROWS_AS(fdb5::Schema("./data/broken_types_missing_semicolon"), fdb5::SchemaParser::Error);
    try {
        fdb5::Schema("./data/broken_types_missing_semicolon");
    }
    catch (fdb5::SchemaParser::Error& ex) {
        std::cout << ex.what() << std::endl;
        EXPECT(std::string(ex.what()).find("SchemaParser::parseTypes") != std::string::npos);
    }
}

CASE("Broken schema - No type name") {
    EXPECT_THROWS_AS(fdb5::Schema("./data/broken_types_no_name"), fdb5::SchemaParser::Error);
    try {
        fdb5::Schema("./data/broken_types_no_name");
    }
    catch (fdb5::SchemaParser::Error& ex) {
        std::cout << ex.what() << std::endl;
        EXPECT(std::string(ex.what()).find("Error parsing rules") != std::string::npos);
    }
}

CASE("Broken schema - No type type") {
    EXPECT_THROWS_AS(fdb5::Schema("./data/broken_types_no_type"), fdb5::SchemaParser::Error);
    try {
        fdb5::Schema("./data/broken_types_no_type");
    }
    catch (fdb5::SchemaParser::Error& ex) {
        std::cout << ex.what() << std::endl;
        EXPECT(std::string(ex.what()).find("SchemaParser::parseTypes") != std::string::npos);
    }
}

CASE("Broken schema - Missing closing bracket") {

    EXPECT_THROWS_AS(fdb5::Schema("./data/schema_incomplete_rule"), fdb5::SchemaParser::Error);
    try {
        fdb5::Schema("./data/schema_incomplete_rule");
    }
    catch (fdb5::SchemaParser::Error& ex) {
        std::cout << ex.what() << std::endl;
        EXPECT(std::string(ex.what()).find("SchemaParser::parseDatabase") != std::string::npos);
    }
}

CASE("Broken schema - Non-ASCII chars inside rule") {

    EXPECT_THROWS_AS(fdb5::Schema("./data/non_ascii_chars"), fdb5::SchemaParser::Error);
    try {
        fdb5::Schema("./data/non_ascii_chars");
    }
    catch (fdb5::SchemaParser::Error& ex) {
        std::cout << ex.what() << std::endl;
        EXPECT(std::string(ex.what()).find("non-ASCII") != std::string::npos);
    }
}

CASE("Broken schema - Non-ASCII chars before rule") {

    EXPECT_THROWS_AS(fdb5::Schema("./data/non_ascii_before_rule"), fdb5::SchemaParser::Error);
    try {
        fdb5::Schema("./data/non_ascii_before_rule");
    }
    catch (fdb5::SchemaParser::Error& ex) {
        std::cout << ex.what() << std::endl;
        EXPECT(std::string(ex.what()).find("non-ASCII") != std::string::npos);
    }
}


CASE("Correct schema - Production Schema") {

    EXPECT_NO_THROW(fdb5::Schema("./data/schema"));
}


//----------------------------------------------------------------------------------------------------------------------

}  // anonymous namespace

int main(int argc, char** argv) {

    eckit::Log::info() << ::getenv("FDB_HOME") << std::endl;

    return ::eckit::testing::run_tests(argc, argv);
}
