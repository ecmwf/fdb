/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// Unit tests for FDB-331: keyword-based file-space matching using metkit::mars::Matcher.

#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"
#include "fdb5/toc/FileSpace.h"
#include "fdb5/toc/RootManager.h"

#include "eckit/config/LocalConfiguration.h"
#include "eckit/config/YAMLConfiguration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/TmpDir.h"
#include "eckit/testing/Test.h"

namespace fdb5::test {

//----------------------------------------------------------------------------------------------------------------------

namespace {

eckit::LocalConfiguration makeMatchConfig() {
    // Equivalent YAML:
    //   match:
    //     class: od
    //     expver: '0001'
    //     stream: [scda, scwv, oper, wave, enfo, waef]
    eckit::LocalConfiguration match;
    match.set("class", "od");
    match.set("expver", std::string{"0001"});
    match.set("stream", std::vector<std::string>{"scda", "scwv", "oper", "wave", "enfo", "waef"});
    eckit::LocalConfiguration cfg;
    cfg.set("match", match);
    return cfg;
}

fdb5::FileSpace makeMatchSpace() {
    return fdb5::FileSpace("kw", makeMatchConfig(), {});
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------
// Matcher-backed FileSpace: full key with allowed value matches

CASE("Matcher: full key with allowed value matches") {
    auto space = makeMatchSpace();

    fdb5::Key key{{{"class", "od"}, {"expver", "0001"}, {"stream", "oper"}, {"date", "20241118"}}};
    EXPECT(space.match(key));
}

CASE("Matcher: present keyword with disallowed value does not match") {
    auto space = makeMatchSpace();

    fdb5::Key key{{{"class", "od"}, {"expver", "0001"}, {"stream", "eefo"}, {"date", "20241118"}}};
    EXPECT(!space.match(key));
}

CASE("Matcher: missing keyword does not disqualify the space (FDB-331, MatchOnMissing)") {
    auto space = makeMatchSpace();

    // No 'stream' keyword at all -- the partial request that broke fdb-list.
    fdb5::Key key{{{"class", "od"}, {"expver", "0001"}, {"date", "20241118"}, {"time", "0000"}}};
    EXPECT(space.match(key));
}

CASE("Matcher: missing keyword disqualifies with DontMatchOnMissing") {
    auto space = makeMatchSpace();

    // The same partial key, but with DontMatchOnMissing — the absent
    // 'stream' keyword now causes the space to be excluded.
    fdb5::Key key{{{"class", "od"}, {"expver", "0001"}, {"date", "20241118"}, {"time", "0000"}}};
    EXPECT(!space.match(key));
}

CASE("Matcher: empty keyword value is treated as absent (FDB-331)") {
    // Schema::matchDatabase() produces partial Keys whose absent dimensions
    // are present-but-empty. The KeyAccessor treats such placeholders as
    // "absent", so MatchOnMissing lets them through.
    auto space = makeMatchSpace();

    fdb5::Key key;
    key.set("class", "od");
    key.set("expver", "0001");
    key.set("stream", "");  // <-- placeholder, not a real value
    EXPECT(space.match(key));
}

CASE("Matcher: scalar value mismatch fails") {
    auto space = makeMatchSpace();

    fdb5::Key key;
    key.set("class", "rd");
    key.set("expver", "0001");
    EXPECT(!space.match(key));
}

CASE("Matcher: empty key matches with MatchOnMissing (no constraints can be violated)") {
    auto space = makeMatchSpace();

    fdb5::Key key;
    EXPECT(space.match(key));
}

//----------------------------------------------------------------------------------------------------------------------

CASE("FileSpace: regex backend still works on stringified key") {
    eckit::LocalConfiguration cfg;
    cfg.set("regex", std::string{"^od:(0001):.*"});
    cfg.set("handler", std::string{"Default"});
    fdb5::FileSpace space("regex_only", cfg, {});

    fdb5::Key key{{{"class", "od"}, {"expver", "0001"}, {"stream", "oper"}}};
    EXPECT(space.match(key));

    fdb5::Key kRd{{{"class", "rd"}, {"expver", "0001"}, {"stream", "oper"}}};
    EXPECT(!space.match(kRd));
}

CASE("FileSpace: matcher backend recovers partial requests") {
    auto space = makeMatchSpace();

    fdb5::Key kFull{{{"class", "od"}, {"expver", "0001"}, {"stream", "oper"}}};
    EXPECT(space.match(kFull));

    // The FDB-331 reproducer: request without stream still matches the
    // first space, so its roots will be visited.
    fdb5::Key kPartial{{{"class", "od"}, {"expver", "0001"}, {"date", "20241118"}, {"time", "0000"}}};
    EXPECT(space.match(kPartial));
}

//----------------------------------------------------------------------------------------------------------------------
// Input validation

CASE("buildMatcher: keyword with empty value list throws UserError") {
    eckit::LocalConfiguration match;
    match.set("stream", std::vector<std::string>{});
    eckit::LocalConfiguration cfg;
    cfg.set("match", match);
    EXPECT_THROWS_AS(fdb5::FileSpace("test", cfg, {}), eckit::UserError);
}

//----------------------------------------------------------------------------------------------------------------------
// Read YAML

CASE("buildMatcher: builds correctly from YAML (mixed scalar + list)") {
    const std::string yaml =
        "match:\n"
        "  class: od\n"
        "  expver: '0001'\n"
        "  stream: [scda, scwv, oper]\n";
    eckit::YAMLConfiguration yamlCfg{yaml};
    eckit::LocalConfiguration cfg{yamlCfg};

    fdb5::FileSpace space("yaml_test", cfg, {});

    fdb5::Key key{{{"class", "od"}, {"expver", "0001"}, {"stream", "oper"}}};
    EXPECT(space.match(key));

    fdb5::Key keyOther{{{"class", "od"}, {"expver", "0001"}, {"stream", "eefo"}}};
    EXPECT(!space.match(keyOther));
}

//----------------------------------------------------------------------------------------------------------------------
// RootManager

CASE("RootManager: visitableRoots() uses match: and handles partial keys (FDB-331)") {
    eckit::TmpDir root1{eckit::LocalPathName::cwd().c_str()};
    eckit::TmpDir root2{eckit::LocalPathName::cwd().c_str()};

    // Mirrors the FDB-331 production layout: a "primary" space narrowed
    // by stream, and a "fallback" space matching any od/0001 key.
    const std::string yaml =
        "type: local\n"
        "engine: toc\n"
        "spaces:\n"
        " - handler: Default\n"
        "   match:\n"
        "     class: od\n"
        "     expver: '0001'\n"
        "     stream: [scda, scwv, oper, wave, enfo, waef]\n"
        "   roots:\n"
        "    - path: " +
        root1.asString() +
        "\n"
        " - handler: Default\n"
        "   match:\n"
        "     class: od\n"
        "     expver: '0001'\n"
        "   roots:\n"
        "    - path: " +
        root2.asString() + "\n";

    fdb5::Config config{eckit::YAMLConfiguration{yaml}};
    fdb5::CatalogueRootManager mgr{config};

    // Full key with an allowed stream: both spaces match (primary by name,
    // fallback unconstrained on stream) -- both roots are visited.
    fdb5::Key kFull{{{"class", "od"}, {"expver", "0001"}, {"stream", "oper"}, {"date", "20241118"}}};
    auto rootsFull = mgr.visitableRoots(kFull);
    EXPECT_EQUAL(rootsFull.size(), 2U);

    // Full key with a NON-allowed stream: only the fallback matches.
    fdb5::Key kEefo{{{"class", "od"}, {"expver", "0001"}, {"stream", "eefo"}, {"date", "20241118"}}};
    auto rootsEefo = mgr.visitableRoots(kEefo);
    EXPECT_EQUAL(rootsEefo.size(), 1U);
    EXPECT_EQUAL(rootsEefo.front(), eckit::PathName{root2.asString()});

    // FDB-331 reproducer: stream is missing entirely. With the existing
    // regex backend the primary space would be silently dropped here
    // (regex on the stringified key cannot tell "absent" apart from
    // "empty"); with match: both spaces remain visitable.
    fdb5::Key kPartial{{{"class", "od"}, {"expver", "0001"}, {"date", "20241118"}}};
    auto rootsPartial = mgr.visitableRoots(kPartial);
    EXPECT_EQUAL(rootsPartial.size(), 2U);

    // A different class matches neither space.
    fdb5::Key kRd{{{"class", "rd"}, {"expver", "0001"}, {"stream", "oper"}}};
    auto rootsRd = mgr.visitableRoots(kRd);
    EXPECT(rootsRd.empty());
}

CASE("RootManager: rejects spaces with both regex: and match:") {
    const std::string yaml =
        "type: local\n"
        "engine: toc\n"
        "spaces:\n"
        " - handler: Default\n"
        "   regex: ^od:.*\n"
        "   match:\n"
        "     class: od\n"
        "   roots:\n"
        "    - path: /tmp\n";

    fdb5::Config config{eckit::YAMLConfiguration{yaml}};
    EXPECT_THROWS_AS(fdb5::CatalogueRootManager{config}, eckit::UserError);
}

}  // namespace fdb5::test

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char** argv) {
    return eckit::testing::run_tests(argc, argv);
}
