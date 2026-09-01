/*
 * (C) Copyright 2026- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"
#include "fdb5/toc/TocHandler.h"

#include "eckit/filesystem/PathName.h"
#include "eckit/log/Log.h"
#include "eckit/testing/Test.h"

#include <cstdlib>

namespace fdb5::test {

//----------------------------------------------------------------------------------------------------------------------

namespace {

/// Directory on a real NFS mount, as provided by the test harness (e.g. a docker nfs-server
/// export mounted at FDB_TEST_NFS_MOUNT). Empty when unset, so the NFS-backed cases self-skip in
/// environments without NFS (normal CI).
eckit::PathName nfsMountDir() {
    const char* mount = ::getenv("FDB_TEST_NFS_MOUNT");
    if (mount == nullptr || mount[0] == '\0') {
        return {};
    }
    return eckit::PathName{mount};
}

}  // namespace

CASE("Detection matches the filesystem the directory actually resides on") {
    eckit::PathName tmp = eckit::PathName::unique("nfs_detect_dir");
    tmp.mkdir();

    EXPECT(fdb5::TocHandler::onNFS(tmp) == (tmp.fileSystemType() == "nfs"));

    tmp.rmdir();
}

CASE("A non-existent path fails open to non-NFS") {
    eckit::PathName missing = eckit::PathName::unique("nfs_missing_dir");
    EXPECT(fdb5::TocHandler::onNFS(missing) == false);
}

CASE("TOC init/read round-trips on a main TOC") {
    // n.b. the (path, parentKey) overload constructs a *sub* TOC, and every NFS branch is
    // guarded by !isSubToc_; the (directory, config) overload below is the main-TOC path,
    // so this exercises the code the NFS handling actually applies to.
    eckit::PathName dir = eckit::PathName::unique("nfs_toc_dir");
    dir.mkdir();

    fdb5::Config config = fdb5::Config().expandConfig();
    fdb5::Key key{{{"class", "od"}, {"expver", "0001"}, {"stream", "oper"}}};

    {
        fdb5::TocHandler writer(dir, config);
        writer.writeInitRecord(key);
    }

    {
        fdb5::TocHandler reader(dir, config);
        EXPECT(reader.exists());
        EXPECT_EQUAL(reader.databaseKey(), key);
    }

    // A second creator must observe the initialised TOC rather than re-initialising it.
    {
        fdb5::TocHandler again(dir, config);
        again.writeInitRecord(key);
        EXPECT_EQUAL(again.databaseKey(), key);
    }

    (dir / "toc").unlink();
    (dir / "schema").unlink();
    dir.rmdir();
}

//----------------------------------------------------------------------------------------------------------------------

CASE("NFS mount: detection fires and a TOC round-trips on a real NFS export") {
    const eckit::PathName mount = nfsMountDir();
    if (mount.path().empty() || !mount.exists()) {
        eckit::Log::info() << "NFS mount (FDB_TEST_NFS_MOUNT) not available -- skipping NFS-backed test" << std::endl;
        return;
    }

    // The mount is present, so it must really be NFS; otherwise the harness is misconfigured
    // and this case would validate the wrong filesystem.
    EXPECT_EQUAL(mount.fileSystemType(), std::string("nfs"));

    eckit::PathName dir = eckit::PathName::unique(mount / "fdb_nfs_test");
    dir.mkdir();

    // Detection must fire on a genuine NFS mount (the local unit case above only checks agreement).
    EXPECT(fdb5::TocHandler::onNFS(dir));

    fdb5::Config config = fdb5::Config().expandConfig();
    fdb5::Key key{{{"class", "od"}, {"expver", "0001"}, {"stream", "oper"}}};

    // Exercises the NFS branch (onNFS() && !isSubToc_): locked append on write, locked read on open.
    {
        fdb5::TocHandler writer(dir, config);
        writer.writeInitRecord(key);
    }

    {
        fdb5::TocHandler reader(dir, config);
        EXPECT(reader.exists());
        EXPECT_EQUAL(reader.databaseKey(), key);
    }

    (dir / "toc").unlink();
    (dir / "schema").unlink();
    dir.rmdir();
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::test

int main(int argc, char** argv) {
    return ::eckit::testing::run_tests(argc, argv);
}
