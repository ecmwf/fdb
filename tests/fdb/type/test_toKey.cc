/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <cstdlib>
#include <sstream>

#include "eckit/testing/Test.h"

#include "fdb5/config/Config.h"
#include "fdb5/database/ArchiveVisitor.h"
#include "fdb5/database/Archiver.h"
#include "fdb5/database/Key.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/types/TypeYearMonth.h"

using namespace eckit::testing;
using namespace eckit;

namespace fdb::test {

fdb5::Config config;

char data[4];

CASE("ClimateDaily - no expansion") {

    fdb5::Key key;
    EXPECT_EQUAL(key.valuesToString(), "");
    EXPECT_THROWS(key.value("date"));

    key.set("date", "20210427");
    EXPECT_NO_THROW(key.value("date"));
    EXPECT_EQUAL(key.value("date"), "20210427");
    EXPECT_EQUAL(key.valuesToString(), "20210427");

    key.set("stream", "dacl");
    EXPECT_EQUAL(key.value("date"), "20210427");
    EXPECT_EQUAL(key.valuesToString(), "20210427:dacl");
}

CASE("Step & ClimateDaily - expansion") {
    fdb5::Key key;

    {
        fdb5::TypedKey tKey(config.schema().registry());
        EXPECT_EQUAL(tKey.canonical().valuesToString(), "");
        EXPECT_THROWS(tKey["date"]);

        tKey.set("date", "20210427");
        EXPECT_NO_THROW(key = tKey.canonical());
        EXPECT_EQUAL(key["date"], "20210427");
        EXPECT_EQUAL(key.valuesToString(), "20210427");

        tKey.set("stream", "dacl");
        EXPECT_NO_THROW(key = tKey.canonical());
        EXPECT_EQUAL(key["date"], "20210427");
        EXPECT_EQUAL(key.valuesToString(), "20210427:dacl");

        tKey.set("time", "12:aa");
        EXPECT_THROWS(tKey.canonical());

        tKey.set("time", "12am");
        EXPECT_THROWS(tKey.canonical());

        tKey.set("time", "123");
        EXPECT_EQUAL(tKey.canonical()["time"], "0123");

        tKey.set("time", "1:23");
        EXPECT_EQUAL(tKey.canonical()["time"], "0123");

        tKey.set("time", "01::23::45");
        EXPECT_THROWS(tKey.canonical());

        tKey.set("time", ":01:23:45:");
        EXPECT_THROWS(tKey.canonical());

        tKey.set("time", "12:99");
        EXPECT_THROWS(tKey.canonical());

        tKey.set("time", "7700");
        EXPECT_THROWS(tKey.canonical());

        tKey.set("time", "01:23:45:67");
        EXPECT_THROWS(tKey.canonical());

        tKey.set("time", "12");
        EXPECT_EQUAL(tKey.canonical()["time"], "1200");

        tKey.set("time", "6");
        EXPECT_EQUAL(tKey.canonical()["time"], "0600");

        tKey.set("time", "06:21");
        EXPECT_EQUAL(tKey.canonical()["time"], "0621");

        tKey.set("time", "00:18:00");
        EXPECT_EQUAL(tKey.canonical()["time"], "0018");

        tKey.set("time", "00");
        EXPECT_EQUAL(tKey.canonical()["time"], "0000");

        tKey.set("time", "0");
        EXPECT_EQUAL(tKey.canonical()["time"], "0000");

        tKey.set("time", "00:00");
        EXPECT_EQUAL(tKey.canonical()["time"], "0000");

        tKey.set("time", "00:00:00");
        EXPECT_NO_THROW(key = tKey.canonical());
        EXPECT_EQUAL(key["time"], "0000");
        EXPECT_EQUAL(key.valuesToString(), "20210427:dacl:0000");

        tKey.set("class", "ei");
        tKey.set("expver", "7799");
        tKey.set("domain", "g");
        tKey.set("type", "pb");
        tKey.set("levtype", "pl");
        tKey.set("step", "02-12");
        tKey.set("quantile", "99:100");
        tKey.set("levelist", "50");
        tKey.set("param", "129.128");

        EXPECT_NO_THROW(key = tKey.canonical());
        EXPECT_EQUAL(key.valuesToString(), "20210427:dacl:0000:ei:7799:g:pb:pl:2-12:99:100:50:129.128");
    }

    fdb5::Config conf = config.expandConfig();
    fdb5::Archiver archiver(conf);
    auto visitor = fdb5::ArchiveVisitor::create(archiver, key, data, 4);
    config.schema().expand(key, *visitor);

    fdb5::TypedKey tKey(visitor->rule()->registry());
    tKey.pushFrom(key);

    EXPECT_NO_THROW(key = tKey.canonical());
    EXPECT_EQUAL(key["date"], "0427");
    EXPECT_EQUAL(key["time"], "0000");
    EXPECT_EQUAL(key["step"], "2-12");
    EXPECT_EQUAL(key.valuesToString(), "0427:dacl:0000:ei:7799:g:pb:pl:2-12:99:100:50:129.128");

    tKey.set("step", "00");
    EXPECT_EQUAL(tKey.canonical()["step"], "0");

    tKey.set("step", "1");
    EXPECT_EQUAL(tKey.canonical()["step"], "1");

    tKey.set("step", "0-1");
    EXPECT_EQUAL(tKey.canonical()["step"], "0-1");

    tKey.set("step", "30m");
    EXPECT_EQUAL(tKey.canonical()["step"], "30m");

    tKey.set("step", "60m");
    EXPECT_EQUAL(tKey.canonical()["step"], "1");

    tKey.set("step", "30m-60m");
    EXPECT_EQUAL(tKey.canonical()["step"], "30m-1");

    tKey.set("step", "30m-1");
    EXPECT_EQUAL(tKey.canonical()["step"], "30m-1");

    tKey.set("step", "60m-120m");
    EXPECT_EQUAL(tKey.canonical()["step"], "1-2");
}

CASE("Levelist") {

    fdb5::Key key;

    eckit::DenseSet<std::string> values;
    values.insert("100");
    values.insert("200");
    values.insert("925");
    values.insert("0.05");
    values.insert("0.7");
    values.insert("0.333333");
    values.sort();

    fdb5::TypedKey tKey(config.schema().registry());

    EXPECT_NO_THROW(key = tKey.canonical());
    EXPECT_EQUAL(key.valuesToString(), "");
    EXPECT_THROWS(key["levelist"]);

    tKey.set("levelist", "925");
    EXPECT_NO_THROW(key = tKey.canonical());
    EXPECT_NO_THROW(key["levelist"]);
    EXPECT_EQUAL(key["levelist"], "925");
    EXPECT(key.matchValues("levelist", values));

    tKey.set("levelist", "200.0");
    EXPECT_NO_THROW(key = tKey.canonical());
    EXPECT_EQUAL(key["levelist"], "200");
    EXPECT(key.matchValues("levelist", values));

    tKey.set("levelist", "200.0000000");
    EXPECT_NO_THROW(key = tKey.canonical());
    EXPECT_EQUAL(key["levelist"], "200");
    EXPECT(key.matchValues("levelist", values));

    tKey.set("levelist", "200.1");
    EXPECT_NO_THROW(key = tKey.canonical());
    EXPECT_EQUAL(key["levelist"], "200.1");
    EXPECT(!key.matchValues("levelist", values));

    tKey.set("levelist", "300");
    EXPECT_NO_THROW(key = tKey.canonical());
    EXPECT_EQUAL(key["levelist"], "300");
    EXPECT(!key.matchValues("levelist", values));

    tKey.set("levelist", "0.7");
    EXPECT_NO_THROW(key = tKey.canonical());
    EXPECT_EQUAL(key["levelist"], "0.7");
    EXPECT(key.matchValues("levelist", values));

    tKey.set("levelist", "0.7000");
    EXPECT_NO_THROW(key = tKey.canonical());
    EXPECT_EQUAL(key["levelist"], "0.7");
    EXPECT(key.matchValues("levelist", values));

    tKey.set("levelist", "0.5");
    EXPECT_NO_THROW(key = tKey.canonical());
    EXPECT_EQUAL(key["levelist"], "0.5");
    EXPECT(!key.matchValues("levelist", values));

    tKey.set("levelist", "0.333");
    EXPECT_NO_THROW(key = tKey.canonical());
    EXPECT_EQUAL(key["levelist"], "0.333");
    EXPECT(!key.matchValues("levelist", values));

    tKey.set("levelist", "0.333333");
    EXPECT_NO_THROW(key = tKey.canonical());
    EXPECT_EQUAL(key["levelist"], "0.333333");
    EXPECT(key.matchValues("levelist", values));

    // this works (probably shouldn't), simply becasue to_string uses the same precision as printf %f (default 6)
    /// @note don't use to_string when canonicalising Keys
    tKey.set("levelist", std::to_string(double(1. / 3.)));
    EXPECT_NO_THROW(key = tKey.canonical());
    EXPECT_EQUAL(key["levelist"], "0.333333");
    EXPECT(key.matchValues("levelist", values));
}

CASE("Expver, Time & ClimateDaily - string ctor - expansion") {

    fdb5::Key key;

    {
        auto parsed = fdb5::Key::parse(
            "class=ei,expver=1,stream=dacl,domain=g,type=pb,levtype=pl,date="
            "20210427,time=6,step=0,quantile=99:100,"
            "levelist=50,param=129.128");
        fdb5::TypedKey tKey(config.schema().registry());
        tKey.pushFrom(parsed);
        key = tKey.tidy();
    }

    EXPECT_EQUAL(key["date"], "20210427");
    EXPECT_EQUAL(key["time"], "0600");
    EXPECT_EQUAL(key.valuesToString(), "ei:0001:dacl:g:pb:pl:20210427:0600:0:99:100:50:129.128");

    {
        fdb5::Archiver archiver;
        auto visitor = fdb5::ArchiveVisitor::create(archiver, key, data, 4);
        config.schema().expand(key, *visitor);
        fdb5::TypedKey tKey(visitor->rule()->registry());
        tKey.pushFrom(key);
        EXPECT_NO_THROW(key = tKey.canonical());
    }

    EXPECT_EQUAL(key["date"], "0427");
    EXPECT_EQUAL(key.valuesToString(), "ei:0001:dacl:g:pb:pl:0427:0600:0:99:100:50:129.128");
}

CASE("ClimateMonthly - string ctor - expansion") {

    fdb5::Key key;

    {
        auto parsed = fdb5::Key::parse(
            "class=op,expver=1,stream=mnth,domain=g,type=cl,levtype=pl,date=20210427,time="
            "0000,levelist=50,param=129.128");
        fdb5::TypedKey tKey(config.schema().registry());
        tKey.pushFrom(parsed);
        key = tKey.tidy();
    }

    EXPECT_EQUAL(key["date"], "20210427");
    EXPECT_EQUAL(key.valuesToString(), "op:0001:mnth:g:cl:pl:20210427:0000:50:129.128");

    {
        fdb5::Archiver archiver;
        auto visitor = fdb5::ArchiveVisitor::create(archiver, key, data, 4);
        config.schema().expand(key, *visitor);
        fdb5::TypedKey tKey(visitor->rule()->registry());
        tKey.pushFrom(key);
        EXPECT_NO_THROW(key = tKey.canonical());
    }

    EXPECT_EQUAL(key["date"], "4");
    EXPECT_EQUAL(key.valuesToString(), "op:0001:mnth:g:cl:pl:4:0000:50:129.128");
}

// do we need to keep this behaviour? should we rely on metkit for date expansion and remove it from Key?
CASE("Date - string ctor - expansion") {

    fdb5::Key key;

    {
        auto parsed =
            fdb5::Key::parse("class=od,expver=1,stream=oper,type=ofb,date=-2,time=0000,obsgroup=MHS,reportype=3001");
        fdb5::TypedKey tKey(config.schema().registry());
        tKey.pushFrom(parsed);
        key = tKey.tidy();
    }

    eckit::Date now(-2);

    eckit::Translator<long, std::string> t;

    EXPECT_EQUAL(key["date"], t(now.yyyymmdd()));
    EXPECT_EQUAL(key.valuesToString(), "od:0001:oper:ofb:" + t(now.yyyymmdd()) + ":0000:mhs:3001");

    {
        fdb5::Archiver archiver;
        auto visitor = fdb5::ArchiveVisitor::create(archiver, key, data, 4);
        config.schema().expand(key, *visitor);
        fdb5::TypedKey tKey(visitor->rule()->registry());
        tKey.pushFrom(key);
        EXPECT_NO_THROW(key = tKey.canonical());
    }

    EXPECT_EQUAL(key["date"], t(now.yyyymmdd()));
    EXPECT_EQUAL(key.valuesToString(), "od:0001:oper:ofb:" + t(now.yyyymmdd()) + ":0000:mhs:3001");
}

CASE("YearMonth - toKey") {
    fdb5::TypeYearMonth type("date", "YearMonth");

    EXPECT_EQUAL(type.toKey("20210427"), "202104");
    EXPECT_EQUAL(type.toKey("20021201"), "200212");
    EXPECT_EQUAL(type.toKey("20030101"), "200301");
    EXPECT_EQUAL(type.toKey("20211231"), "202112");
    EXPECT_EQUAL(type.toKey("20210101"), "202101");
}

// FDB-691: a custom, self-contained schema is used here (rather than the shared
// tests/fdb/etc/fdb/schema) so this test doesn't depend on, or need to modify, the
// schema used by every other CASE in this file.
CASE("YearMonth - string ctor - expansion") {

    fdb5::Key key;

    fdb5::Config yearMonthConfig;
    {
        std::istringstream schemaStream(
            "[ class, expver, stream=wamo, domain\n"
            "       [ type, levtype\n"
            "               [ date: YearMonth, time, step?, param ]]\n"
            "]\n");
        yearMonthConfig.overrideSchema("test_toKey_YearMonth_schema", new fdb5::Schema(schemaStream));
    }


    // Check string output of tidy is the same as in the original request
    {
        auto parsed = fdb5::Key::parse(
            "class=ea,expver=0001,stream=wamo,domain=g,type=an,levtype=sfc,date=20200601,time=0000,step=0,param="
            "140254");
        fdb5::TypedKey tKey(yearMonthConfig.schema().registry());
        tKey.pushFrom(parsed);
        key = tKey.tidy();
    }

    EXPECT_EQUAL(key["date"], "20200601");
    EXPECT_EQUAL(key.valuesToString(), "ea:0001:wamo:g:an:sfc:20200601:0000:0:140254");

    // Check string output of canonical is the expected YYYYMM format which is needed from the Archiver
    {
        // Note: we deliberately don't go through Archiver/ArchiveVisitor here. On
        // write, RuleDatabase::expand() only uses the passed-in schema to match the
        // top-level (database) rule. The index/datum rules are re-resolved from
        // the WriteVisitor's *actual, on-disk* catalogue schema
        // (BaseArchiveVisitor::databaseSchema() -> catalogue()->schema()), which
        // would sidestep this self-contained schema entirely. Schema::matchingRule()
        // gives direct access to the matched RuleDatum (and its registry) without
        // needing a real catalogue.
        const fdb5::RuleDatum& rule = yearMonthConfig.schema().matchingRule(key, key);
        fdb5::TypedKey tKey(rule.registry());
        tKey.pushFrom(key);
        EXPECT_NO_THROW(key = tKey.canonical());
    }

    EXPECT_EQUAL(key["date"], "202006");
    EXPECT_EQUAL(key.valuesToString(), "ea:0001:wamo:g:an:sfc:202006:0000:0:140254");
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb::test

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
