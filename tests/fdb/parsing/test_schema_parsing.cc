
#include <fstream>

#include "eckit/testing/Test.h"
#include "fdb5/api/exceptions/SchemaError.h"
#include "fdb5/rules/Predicate.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/rules/SchemaParser.h"
#include "fdb5/types/TypesRegistry.h"

namespace {

// Builds the schema at `path`, expects it to throw ExceptionT, and checks
// that the exception message contains `expectedSubstr`. Replaces the old
// pattern of constructing the schema twice per case (once to check the
// type, once more to check the message).
template <typename ExceptionT>
void expectSchemaError(const std::string& path, const std::string& expectedSubstr) {
    bool thrown = false;

    try {
        fdb5::Schema schema(path);
    }
    catch (ExceptionT& ex) {
        thrown = true;
        std::cout << ex.what() << std::endl;
        EXPECT(std::string(ex.what()).find(expectedSubstr) != std::string::npos);
    }

    EXPECT(thrown);
}

// Same as expectSchemaError, but drives fdb5::SchemaParser directly off a
// std::istream (rather than fdb5::Schema off a file path), the way callers
// with an in-memory schema do. Used to check that the stream-based
// SchemaParser ctor reports the same errors as the file-based one, just
// without a real path in the message (SchemaParser has no path to report,
// so it falls back to a fixed "Created from std::istream" marker).
template <typename ExceptionT>
void expectSchemaErrorFromStream(const std::string& path, const std::string& expectedSubstr) {
    std::ifstream in(path);
    EXPECT(in);

    bool thrown = false;
    fdb5::RuleList rules;
    fdb5::TypesRegistry registry;

    try {
        fdb5::SchemaParser parser(in);
        parser.parse(rules, registry);
    }
    catch (ExceptionT& ex) {
        thrown = true;
        std::cout << ex.what() << std::endl;
        EXPECT(std::string(ex.what()).find(expectedSubstr) != std::string::npos);
        EXPECT(std::string(ex.what()).find("Created from std::istream") != std::string::npos);
    }

    EXPECT(thrown);
}

CASE("Broken schema - Non-existing schema file") {
    // No stream counterpart: a std::istream is already open when handed to
    // SchemaParser, so there is no "file not found" case on that path.
    expectSchemaError<eckit::CantOpenFile>("./data/non-existing", "Cannot open");
}

CASE("Broken schema - No Rule") {
    expectSchemaError<fdb5::SchemaError>("./data/broken_schema_no_rule", "SchemaParser::parse: Empty rule list");
}

CASE("Broken schema (stream) - No Rule") {
    expectSchemaErrorFromStream<fdb5::SchemaError>("./data/broken_schema_no_rule",
                                                   "SchemaParser::parse: Empty rule list");
}

CASE("Broken schema - No Rule but comments") {
    expectSchemaError<fdb5::SchemaError>("./data/broken_schema_comments_no_rule",
                                         "SchemaParser::parse: Empty rule list");
}

CASE("Broken schema (stream) - No Rule but comments") {
    expectSchemaErrorFromStream<fdb5::SchemaError>("./data/broken_schema_comments_no_rule",
                                                   "SchemaParser::parse: Empty rule list");
}

CASE("Broken schema - Missing semicolon types") {
    expectSchemaError<fdb5::SchemaError>("./data/broken_types_missing_semicolon", "SchemaParser::parseTypes");
}

CASE("Broken schema (stream) - Missing semicolon types") {
    expectSchemaErrorFromStream<fdb5::SchemaError>("./data/broken_types_missing_semicolon", "SchemaParser::parseTypes");
}

CASE("Broken schema - No type name") {
    expectSchemaError<fdb5::SchemaError>("./data/broken_types_no_name", "Error parsing rules");
}

CASE("Broken schema (stream) - No type name") {
    expectSchemaErrorFromStream<fdb5::SchemaError>("./data/broken_types_no_name", "Error parsing rules");
}

CASE("Broken schema - No type type") {
    expectSchemaError<fdb5::SchemaError>("./data/broken_types_no_type", "SchemaParser::parseTypes");
}

CASE("Broken schema (stream) - No type type") {
    expectSchemaErrorFromStream<fdb5::SchemaError>("./data/broken_types_no_type", "SchemaParser::parseTypes");
}

CASE("Broken schema - Missing closing bracket") {
    expectSchemaError<fdb5::SchemaError>("./data/schema_incomplete_rule", "SchemaParser::parseDatabase");
}

CASE("Broken schema (stream) - Missing closing bracket") {
    expectSchemaErrorFromStream<fdb5::SchemaError>("./data/schema_incomplete_rule", "SchemaParser::parseDatabase");
}

CASE("Broken schema - Non-ASCII chars inside rule") {
    expectSchemaError<fdb5::SchemaError>("./data/non_ascii_chars", "non-ASCII");
}

CASE("Broken schema (stream) - Non-ASCII chars inside rule") {
    expectSchemaErrorFromStream<fdb5::SchemaError>("./data/non_ascii_chars", "non-ASCII");
}

CASE("Broken schema - Non-ASCII chars before rule") {
    expectSchemaError<fdb5::SchemaError>("./data/non_ascii_before_rule", "non-ASCII");
}

CASE("Broken schema (stream) - Non-ASCII chars before rule") {
    expectSchemaErrorFromStream<fdb5::SchemaError>("./data/non_ascii_before_rule", "non-ASCII");
}


CASE("Correct schema - Production Schema") {

    EXPECT_NO_THROW(fdb5::Schema("./data/schema"));
}

CASE("Correct schema (stream) - Production Schema") {

    std::ifstream in("./data/schema");
    EXPECT(in);

    fdb5::RuleList rules;
    fdb5::TypesRegistry registry;

    EXPECT_NO_THROW(fdb5::SchemaParser(in).parse(rules, registry));
    EXPECT(!rules.empty());
}


//----------------------------------------------------------------------------------------------------------------------

}  // anonymous namespace

int main(int argc, char** argv) {

    eckit::Log::info() << ::getenv("FDB_HOME") << std::endl;

    return ::eckit::testing::run_tests(argc, argv);
}
