/*
 * (C) Copyright 2026- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/Key.h"
#include "fdb5/toc/TocHandler.h"

#include "eckit/filesystem/PathName.h"
#include "eckit/testing/Test.h"

namespace fdb5::test {

//----------------------------------------------------------------------------------------------------------------------

CASE("A local temporary directory is not detected as NFS") {
    eckit::PathName tmp = eckit::PathName::unique("nfs_detect_dir");
    tmp.mkdir();

    EXPECT(fdb5::TocHandler::onNFS(tmp) == false);

    tmp.rmdir();
}

CASE("A non-existent path fails open to non-NFS") {
    eckit::PathName missing = eckit::PathName::unique("nfs_missing_dir");
    EXPECT(fdb5::TocHandler::onNFS(missing) == false);
}

CASE("TOC init/read round-trips on the local path") {
    eckit::PathName dir = eckit::PathName::unique("nfs_toc_dir");
    dir.mkdir();

    eckit::PathName tocPath = dir / "toc";
    fdb5::Key key{{{"class", "od"}, {"expver", "0001"}, {"stream", "oper"}}};

    {
        fdb5::TocHandler writer(tocPath, fdb5::Key{});
        writer.writeInitRecord(key);
    }

    {
        fdb5::TocHandler reader(tocPath, fdb5::Key{});
        EXPECT(reader.exists());
        EXPECT_EQUAL(reader.databaseKey(), key);
    }

    tocPath.unlink();
    dir.rmdir();
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::test

int main(int argc, char** argv) {
    return ::eckit::testing::run_tests(argc, argv);
}
