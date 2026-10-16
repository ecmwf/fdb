/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <sys/stat.h>
#include <sys/time.h>

#include <string>
#include <vector>

#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/config/LocalConfiguration.h"
#include "eckit/filesystem/TmpDir.h"
#include "eckit/testing/Test.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/AccessIterator.h"
#include "fdb5/config/Config.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/api/helpers/StatusIterator.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/toc/TocHandler.h"

namespace fdb5::test {

//----------------------------------------------------------------------------------------------------------------------

namespace {

const std::string dbKeyString = "class=od,expver=xxxx,stream=oper,date=20101010,time=0000,domain=g";
const std::string dbDirName = "od:xxxx:oper:20101010:0000:g";

/// Build a self-contained configuration rooted at @p root.
///
/// n.b. deliberately not driven by environment variables. Every FDB resource is cached in a
/// function-local static, so $FDB_ROOT_DIRECTORY and $FDB_ACCESS_MARKERS are fixed by whichever
/// test case happens to run first. Going through userConfig keeps each case independent, and
/// exercises the config path rather than the override path.
Config makeConfig(const eckit::PathName& root, bool markListAsAccess = false, long granularity = 0) {

    eckit::LocalConfiguration userConfig;
    userConfig.set("markListAsAccess", markListAsAccess);
    userConfig.set("accessTimestampGranularity", granularity);

    eckit::LocalConfiguration rootConfig;
    rootConfig.set("path", root.asString());

    eckit::LocalConfiguration space;
    space.set("handler", "Default");
    space.set("roots", std::vector<eckit::LocalConfiguration>{rootConfig});

    eckit::LocalConfiguration config;
    config.set("type", "local");
    config.set("engine", "toc");
    config.set("spaces", std::vector<eckit::LocalConfiguration>{space});

    return Config(config, userConfig);
}

void archiveSome(FDB& fdb, const std::vector<std::string>& steps = {"1", "2"}) {

    const std::string payload = "Raining cats and dogs";

    Key key;
    key.set("class", "od");
    key.set("expver", "xxxx");
    key.set("stream", "oper");
    key.set("date", "20101010");
    key.set("time", "0000");
    key.set("domain", "g");
    key.set("type", "fc");
    key.set("levtype", "sfc");
    key.set("param", "130");

    for (const auto& step : steps) {
        key.set("step", step);
        fdb.archive(key, payload.c_str(), payload.size());
    }

    fdb.flush();
}

const char* readFile = "read.timestamp";
const char* writeFile = "write.timestamp";

void setTime(const eckit::PathName& directory, const char* name, time_t t) {
    struct timeval times[2];
    times[0].tv_sec = t;
    times[0].tv_usec = 0;
    times[1].tv_sec = t;
    times[1].tv_usec = 0;
    EXPECT(::utimes((directory / name).localPath(), times) == 0);
}

/// Reset both timestamps to a known point in the past.
void setTimes(const eckit::PathName& directory, time_t t) {
    setTime(directory, readFile, t);
    setTime(directory, writeFile, t);
}

mode_t modeOf(const eckit::PathName& directory, const char* name) {
    struct stat info;
    EXPECT(::stat((directory / name).localPath(), &info) == 0);
    return info.st_mode & 07777;
}

time_t lastRead(const eckit::PathName& directory) {
    struct stat info;
    if (::stat((directory / readFile).localPath(), &info) != 0) {
        return 0;
    }
    return info.st_mtime;
}

time_t lastWrite(const eckit::PathName& directory) {
    struct stat info;
    if (::stat((directory / writeFile).localPath(), &info) != 0) {
        return 0;
    }
    return info.st_mtime;
}

/// An arbitrary time well in the past, used as a baseline that any real access must move away from.
constexpr time_t longAgo = 1000000000;  // 2001-09-09

void drain(ListIterator& it) {
    ListElement elem;
    while (it.next(elem)) {}
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

CASE("The timestamp mode follows the permissions FDB grants its own files") {

    eckit::TmpDir tmpdir(eckit::LocalPathName::cwd().c_str());

    // Anyone who can read the data must be able to record that they did, and nobody else. The mode
    // is derived from FDB's own file permissions rather than configured: write is granted exactly
    // where read is.
    {
        eckit::LocalConfiguration userConfig;
        eckit::LocalConfiguration config;
        config.set("type", "local");
        config.set("permissions", "0644");
        EXPECT_EQUAL(TocHandler::timestampMode(Config(config, userConfig).umask()), mode_t(0666));
    }
    {
        eckit::LocalConfiguration userConfig;
        eckit::LocalConfiguration config;
        config.set("type", "local");
        config.set("permissions", "0640");
        EXPECT_EQUAL(TocHandler::timestampMode(Config(config, userConfig).umask()), mode_t(0660));
    }
    {
        eckit::LocalConfiguration userConfig;
        eckit::LocalConfiguration config;
        config.set("type", "local");
        config.set("permissions", "0600");
        EXPECT_EQUAL(TocHandler::timestampMode(Config(config, userConfig).umask()), mode_t(0600));
    }
}

CASE("Archiving creates both timestamps, with the read one updatable by non-owners") {

    eckit::TmpDir tmpdir(eckit::LocalPathName::cwd().c_str());

    FDB fdb(makeConfig(tmpdir));
    archiveSome(fdb);

    const eckit::PathName db = tmpdir / dbDirName;
    EXPECT((db / readFile).exists());
    EXPECT((db / writeFile).exists());

    // n.b. locals: the comma inside modeOf(...) would otherwise split the EXPECT_EQUAL macro.
    const mode_t readPerms = modeOf(db, readFile);
    const mode_t writePerms = modeOf(db, writeFile);

    // The read timestamp is the only file a non-owner ever modifies. Its mode must be forced,
    // because the writer holds an AutoUmask from Config::umask() which would otherwise strip the
    // very write bits that make the mechanism work.
    EXPECT_EQUAL(readPerms, mode_t(0666));

    // The write timestamp is only ever updated by the owner, so it is left to the umask.
    EXPECT_EQUAL(writePerms, mode_t(0644));

    // Neither is a program. (Locals again: << binds tighter than &, so an inline mask would be
    // mis-parsed inside the macro.)
    const mode_t readExec = readPerms & mode_t(0111);
    const mode_t writeExec = writePerms & mode_t(0111);
    EXPECT_EQUAL(readExec, mode_t(0));
    EXPECT_EQUAL(writeExec, mode_t(0));
}

CASE("A database written but never read carries a read time from its creation") {

    eckit::TmpDir tmpdir(eckit::LocalPathName::cwd().c_str());

    FDB fdb(makeConfig(tmpdir));
    archiveSome(fdb);
    const eckit::PathName db = tmpdir / dbDirName;

    // Readers cannot create files in the database directory, so the writer creates both. "Last
    // read" therefore means "last read, or creation if never read", which keeps age arithmetic
    // meaningful without a separate "never" state.
    EXPECT(lastRead(db) != 0);
    EXPECT(lastRead(db) <= lastWrite(db));
}

CASE("Archiving restores missing records, without checking for them on every write") {

    eckit::TmpDir tmpdir(eckit::LocalPathName::cwd().c_str());

    FDB fdb(makeConfig(tmpdir));
    archiveSome(fdb);
    const eckit::PathName db = tmpdir / dbDirName;

    // A database written before the timestamps existed carries no records at all. The write path
    // does not look for them before each update - it repairs only once an update has failed - so
    // the next archive is what must put them back.
    (db / readFile).unlink();
    (db / writeFile).unlink();

    archiveSome(fdb, {"3", "4"});

    EXPECT((db / writeFile).exists());
    EXPECT(lastWrite(db) != 0);

    // Both are restored, not just the one that was being updated: readers cannot create their own.
    EXPECT((db / readFile).exists());

    // And the read record comes back with the mode that lets a non-owner update it, which has to
    // be forced past the writer's umask.
    EXPECT_EQUAL(modeOf(db, readFile), mode_t(0666));
}

CASE("Metadata-only operations count as neither a read nor a write") {

    eckit::TmpDir tmpdir(eckit::LocalPathName::cwd().c_str());

    FDB fdb(makeConfig(tmpdir));
    archiveSome(fdb);
    const eckit::PathName db = tmpdir / dbDirName;

    const FDBToolRequest request = FDBToolRequest::requestsFromString(dbKeyString)[0];

    // This is the property the whole feature rests on: an estate-wide sweep with any of these
    // tools must leave every timestamp untouched, otherwise nothing ever looks cold.
    {
        setTimes(db, longAgo);
        auto it = fdb.list(request);
        drain(it);
        EXPECT_EQUAL(lastRead(db), longAgo);
        EXPECT_EQUAL(lastWrite(db), longAgo);
    }
    {
        setTimes(db, longAgo);
        auto it = fdb.stats(request);
        StatsElement elem;
        while (it.next(elem)) {}
        EXPECT_EQUAL(lastRead(db), longAgo);
        EXPECT_EQUAL(lastWrite(db), longAgo);
    }
    {
        setTimes(db, longAgo);
        auto it = fdb.axesIterator(request, 3);
        AxesElement elem;
        while (it.next(elem)) {}
        EXPECT_EQUAL(lastRead(db), longAgo);
        EXPECT_EQUAL(lastWrite(db), longAgo);
    }
    {
        setTimes(db, longAgo);
        auto it = fdb.status(request);
        StatusElement elem;
        while (it.next(elem)) {}
        EXPECT_EQUAL(lastRead(db), longAgo);
        EXPECT_EQUAL(lastWrite(db), longAgo);
    }
    {
        // A dry-run wipe reads a great deal of metadata, but moves no data.
        setTimes(db, longAgo);
        auto it = fdb.wipe(request, false);
        WipeElement elem;
        while (it.next(elem)) {}
        EXPECT_EQUAL(lastRead(db), longAgo);
        EXPECT_EQUAL(lastWrite(db), longAgo);
    }
}

CASE("Inspect counts as a read and not as a write") {

    eckit::TmpDir tmpdir(eckit::LocalPathName::cwd().c_str());

    FDB fdb(makeConfig(tmpdir));
    archiveSome(fdb);
    const eckit::PathName db = tmpdir / dbDirName;

    const metkit::mars::MarsRequest request =
        FDBToolRequest::requestsFromString(dbKeyString + ",type=fc,levtype=sfc,param=130,step=1")[0].request();

    setTimes(db, longAgo);
    auto it = fdb.inspect(request);
    drain(it);
    EXPECT(lastRead(db) > longAgo);
    EXPECT_EQUAL(lastWrite(db), longAgo);
}

CASE("Archiving counts as a write and not as a read") {

    eckit::TmpDir tmpdir(eckit::LocalPathName::cwd().c_str());

    FDB fdb(makeConfig(tmpdir));
    archiveSome(fdb, {"1"});
    const eckit::PathName db = tmpdir / dbDirName;

    setTimes(db, longAgo);
    archiveSome(fdb, {"2"});
    EXPECT(lastWrite(db) > longAgo);
    EXPECT_EQUAL(lastRead(db), longAgo);
}

CASE("An inspect that matches nothing does not count as access") {

    eckit::TmpDir tmpdir(eckit::LocalPathName::cwd().c_str());

    FDB fdb(makeConfig(tmpdir));
    archiveSome(fdb, {"1"});
    const eckit::PathName db = tmpdir / dbDirName;

    // step=99 was never archived, so no datum is resolved from this database.
    const metkit::mars::MarsRequest request =
        FDBToolRequest::requestsFromString(dbKeyString + ",type=fc,levtype=sfc,param=130,step=99")[0].request();

    setTimes(db, longAgo);
    auto it = fdb.inspect(request);
    drain(it);
    EXPECT_EQUAL(lastRead(db), longAgo);
    EXPECT_EQUAL(lastWrite(db), longAgo);
}

CASE("Listing counts as a read only when opted in") {

    eckit::TmpDir tmpdir(eckit::LocalPathName::cwd().c_str());
    const FDBToolRequest request = FDBToolRequest::requestsFromString(dbKeyString)[0];
    const eckit::PathName db = tmpdir / dbDirName;

    {
        // Default: a listing is metadata-only and leaves both timestamps alone.
        FDB fdb(makeConfig(tmpdir));
        archiveSome(fdb);

        setTimes(db, longAgo);
        auto it = fdb.list(request);
        drain(it);
        EXPECT_EQUAL(lastRead(db), longAgo);
    }
    {
        // Opted in: for clients that bulk-read via list() followed by dataHandle().
        FDB fdb(makeConfig(tmpdir, /* markListAsAccess */ true));

        setTimes(db, longAgo);
        auto it = fdb.list(request);
        drain(it);
        EXPECT(lastRead(db) > longAgo);
        EXPECT_EQUAL(lastWrite(db), longAgo);
    }
}

CASE("Updates are coarsened within the granularity window") {

    eckit::TmpDir tmpdir(eckit::LocalPathName::cwd().c_str());

    FDB fdb(makeConfig(tmpdir, false, /* granularity */ 3600));
    archiveSome(fdb);
    const eckit::PathName db = tmpdir / dbDirName;

    const metkit::mars::MarsRequest request =
        FDBToolRequest::requestsFromString(dbKeyString + ",type=fc,levtype=sfc,param=130,step=1")[0].request();

    {
        auto it = fdb.inspect(request);
        drain(it);
    }
    EXPECT(lastRead(db) != 0);

    // A second read within the window must not rewrite the inode.
    setTime(db, readFile, longAgo);
    {
        auto it = fdb.inspect(request);
        drain(it);
    }
    EXPECT_EQUAL(lastRead(db), longAgo);

    // ... whereas with coarsening disabled it does.
    FDB unthrottled(makeConfig(tmpdir, false, /* granularity */ 0));
    {
        auto it = unthrottled.inspect(request);
        drain(it);
    }
    EXPECT(lastRead(db) > longAgo);
}

CASE("A missing timestamp is unknown, and reading still works") {

    eckit::TmpDir tmpdir(eckit::LocalPathName::cwd().c_str());

    FDB fdb(makeConfig(tmpdir));
    archiveSome(fdb);
    const eckit::PathName db = tmpdir / dbDirName;

    (db / readFile).unlink();
    EXPECT_EQUAL(lastRead(db), 0);

    // Readers must never create a timestamp: they generally cannot write to the directory, and a
    // file created on read would record the wrong thing anyway.
    const metkit::mars::MarsRequest request =
        FDBToolRequest::requestsFromString(dbKeyString + ",type=fc,levtype=sfc,param=130,step=1")[0].request();
    auto it = fdb.inspect(request);
    drain(it);

    EXPECT_NOT((db / readFile).exists());
    EXPECT_EQUAL(lastRead(db), 0);
}

CASE("status reports both timestamps") {

    eckit::TmpDir tmpdir(eckit::LocalPathName::cwd().c_str());

    FDB fdb(makeConfig(tmpdir));
    archiveSome(fdb);
    const eckit::PathName db = tmpdir / dbDirName;
    setTime(db, readFile, longAgo);
    setTime(db, writeFile, longAgo + 1000);

    auto it = fdb.status(FDBToolRequest::requestsFromString(dbKeyString)[0]);
    StatusElement elem;
    size_t count = 0;
    while (it.next(elem)) {
        EXPECT_EQUAL(elem.lastRead, longAgo);
        EXPECT_EQUAL(elem.lastWrite, longAgo + 1000);
        count++;
    }
    EXPECT_EQUAL(count, 1);
}

CASE("touch records a read or a write explicitly") {

    eckit::TmpDir tmpdir(eckit::LocalPathName::cwd().c_str());

    FDB fdb(makeConfig(tmpdir));
    archiveSome(fdb);
    const eckit::PathName db = tmpdir / dbDirName;
    const FDBToolRequest request = FDBToolRequest::requestsFromString(dbKeyString)[0];

    {
        setTimes(db, longAgo);
        auto it = fdb.touch(request, AccessIdentifier::Read);
        StatusElement elem;
        while (it.next(elem)) {}
        EXPECT(lastRead(db) > longAgo);
        EXPECT_EQUAL(lastWrite(db), longAgo);
    }
    {
        setTimes(db, longAgo);
        auto it = fdb.touch(request, AccessIdentifier::Write);
        StatusElement elem;
        while (it.next(elem)) {}
        EXPECT_EQUAL(lastRead(db), longAgo);
        EXPECT(lastWrite(db) > longAgo);
    }
}

CASE("An explicit touch creates a missing record; automatic marking does not") {

    eckit::TmpDir tmpdir(eckit::LocalPathName::cwd().c_str());

    FDB fdb(makeConfig(tmpdir));
    archiveSome(fdb);
    const eckit::PathName db = tmpdir / dbDirName;
    const FDBToolRequest request = FDBToolRequest::requestsFromString(dbKeyString)[0];

    // A database written before the feature existed has no records. A reader must not create them
    // - it generally cannot write to the directory, and the event it would record is the wrong one.
    (db / readFile).unlink();
    const metkit::mars::MarsRequest datum =
        FDBToolRequest::requestsFromString(dbKeyString + ",type=fc,levtype=sfc,param=130,step=1")[0].request();
    {
        auto it = fdb.inspect(datum);
        drain(it);
    }
    EXPECT_NOT((db / readFile).exists());

    // An explicit touch does create it, on behalf of a user who knows an access occurred.
    {
        auto it = fdb.touch(request, AccessIdentifier::Read);
        StatusElement elem;
        while (it.next(elem)) {}
    }
    EXPECT((db / readFile).exists());
    EXPECT(lastRead(db) != 0);

    // ... and it is created updatable by non-owners, like one made at archive time.
    const mode_t perms = modeOf(db, readFile);
    EXPECT_EQUAL(perms, mode_t(0666));
}

CASE("touch records an explicit instant, or the toc's") {

    eckit::TmpDir tmpdir(eckit::LocalPathName::cwd().c_str());

    FDB fdb(makeConfig(tmpdir));
    archiveSome(fdb);
    const eckit::PathName db = tmpdir / dbDirName;
    const FDBToolRequest request = FDBToolRequest::requestsFromString(dbKeyString)[0];

    constexpr time_t chosen = 1000003000;  // an arbitrary instant, distinct from longAgo

    {
        setTimes(db, longAgo);
        auto it = fdb.touch(request, AccessIdentifier::Read | AccessIdentifier::Write, AccessTime::at(chosen));
        StatusElement elem;
        while (it.next(elem)) {}

        // Recorded exactly, not merely advanced.
        EXPECT_EQUAL(lastRead(db), chosen);
        EXPECT_EQUAL(lastWrite(db), chosen);
    }
    {
        // The toc's own instant, which is how an archive predating the feature is seeded without
        // every database appearing freshly accessed.
        setTimes(db, longAgo);
        setTime(db, "toc", chosen + 500);

        auto it = fdb.touch(request, AccessIdentifier::Read, AccessTime::catalogueTime());
        StatusElement elem;
        while (it.next(elem)) {}

        EXPECT_EQUAL(lastRead(db), chosen + 500);
        EXPECT_EQUAL(lastWrite(db), longAgo);
    }
    {
        // The default is still now.
        setTimes(db, longAgo);
        auto it = fdb.touch(request, AccessIdentifier::Read);
        StatusElement elem;
        while (it.next(elem)) {}

        EXPECT(lastRead(db) > chosen + 500);
    }
}

CASE("An explicit touch creates a missing record at the requested instant") {

    eckit::TmpDir tmpdir(eckit::LocalPathName::cwd().c_str());

    FDB fdb(makeConfig(tmpdir));
    archiveSome(fdb);
    const eckit::PathName db = tmpdir / dbDirName;

    constexpr time_t chosen = 1000004000;

    (db / readFile).unlink();

    auto it = fdb.touch(FDBToolRequest::requestsFromString(dbKeyString)[0], AccessIdentifier::Read,
                        AccessTime::at(chosen));
    StatusElement elem;
    while (it.next(elem)) {}

    EXPECT((db / readFile).exists());
    EXPECT_EQUAL(lastRead(db), chosen);
}

CASE("Timestamps do not make a database unwipeable") {

    eckit::TmpDir tmpdir(eckit::LocalPathName::cwd().c_str());

    FDB fdb(makeConfig(tmpdir));
    archiveSome(fdb);
    const eckit::PathName db = tmpdir / dbDirName;
    EXPECT((db / readFile).exists());
    EXPECT((db / writeFile).exists());

    // An entry in the database directory that the catalogue does not account for makes
    // `fdb-wipe --doit` throw "Cannot fully wipe unclean FDB database". Both timestamps must be
    // recognised, and reported as control files rather than as unknowns.
    const FDBToolRequest request = FDBToolRequest::requestsFromString(dbKeyString)[0];

    std::map<WipeElementType, size_t> counts;
    auto dryRun = fdb.wipe(request, false);
    WipeElement elem;
    while (dryRun.next(elem)) {
        counts[elem.type()] += elem.uris().size();
    }
    EXPECT_EQUAL(counts[WipeElementType::UNKNOWN], 0);
    EXPECT_EQUAL(counts[WipeElementType::ERROR], 0);
    EXPECT(counts[WipeElementType::CATALOGUE_CONTROL] >= 2);

    // ... and the real wipe succeeds without --unsafe-wipe-all, removing them with it.
    auto doit = fdb.wipe(request, true);
    while (doit.next(elem)) {}

    EXPECT_NOT(db.exists());
}


}  // namespace fdb5::test

int main(int argc, char** argv) {
    return ::eckit::testing::run_tests(argc, argv);
}
