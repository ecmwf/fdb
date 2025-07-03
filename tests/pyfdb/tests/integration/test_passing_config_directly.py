# (C) Copyright 2011- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import support.util as util
from tests.conftest import create_default_fdb_at


def test_direct_config(setup_fdb_tmp_dir, data_path):
    data = open(data_path / "x138-300.grib", "rb").read()
    tmp_root, fdb = setup_fdb_tmp_dir()
    fdb.archive(data)
    fdb.flush()

    list_output = list(fdb.list(keys=True))
    assert len(list_output) == 1

    # Check that the archive path is in the tmp directory
    # On OSX tmp file paths look like /private/var/folders/.../T/tmp.../x138-300.grib
    # While the tmp directory looks like /var/folders/.../T/tmp.../ hence why this check is not "startwith"
    assert str(tmp_root) in list_output[0]["path"]


def test_opening_two_fdbs(tmp_path, data_path):
    tmp_root1 = tmp_path / "db1"
    tmp_root1.mkdir()
    _, fdb1 = create_default_fdb_at(tmp_root1, data_path)
    tmp_root2 = tmp_path / "db2"
    tmp_root2.mkdir()
    _, fdb2 = create_default_fdb_at(tmp_root2, data_path)

    print(tmp_root1)
    print(tmp_root2)

    for fdb in [fdb1, fdb2]:
        data = open(data_path / "x138-300.grib", "rb").read()
        fdb.archive(data)
        fdb.flush()

    for fdb, root in [(fdb1, tmp_root1), (fdb2, tmp_root2)]:
        list_output = list(fdb.list(keys=True))
        assert len(list_output) == 1
        assert str(root) in list_output[0]["path"]
