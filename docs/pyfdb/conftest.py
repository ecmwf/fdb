# (C) Copyright 2011- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

from findlibs import Path
import pyfdb
import pytest
import pathlib

import itertools
import shutil
import yaml
import git

import eccodes as ec

from doctest import ELLIPSIS

from sybil import Sybil
from sybil.parsers.rest import DocTestParser, PythonCodeBlockParser


def sybil_setup(namespace):
    # there are better ways to do temp directories, but it's a simple example:
    namespace["pyfdb"] = pyfdb


pytest_collect_file = Sybil(
    parsers=[
        DocTestParser(optionflags=ELLIPSIS),
        PythonCodeBlockParser(),
    ],
    patterns=["*.rst", "*.py"],
    fixtures=["fdb_config_path", "data_path", "test_case_tmp"],
    setup=sybil_setup,
).pytest()


def get_git_root(path) -> Path:
    git_repo = git.Repo(path, search_parent_directories=True)
    git_root = git_repo.git.rev_parse("--show-toplevel")
    return Path(git_root)


@pytest.fixture(scope="function")
def data_path(doctest_namespace) -> pathlib.Path:
    """
    Provides path to test data
    """
    path = get_git_root(__file__).resolve() / "tests" / "pyfdb" / "data"
    assert path.exists()
    doctest_namespace["data_path"] = path
    return path


@pytest.fixture(scope="function")
def test_data_path() -> pathlib.Path:
    """
    Provides path to test data
    """

    git_repo = git.Repo(__file__, search_parent_directories=True)
    git_root = pathlib.Path(git_repo.git.rev_parse("--show-toplevel"))
    path = git_root / "tests" / "pyfdb" / "data"
    assert path.exists()
    return path


@pytest.fixture(scope="function")
def test_case_tmp(tmp_path_factory) -> pathlib.Path:
    return tmp_path_factory.mktemp("session_data")


@pytest.fixture(scope="function")
def build_grib_messages(data_path, test_case_tmp) -> pathlib.Path:
    template_grib = data_path / "template.grib"
    assert template_grib.is_file()
    template_grib_fd = open(template_grib, "rb")
    gid = ec.codes_grib_new_from_file(template_grib_fd)
    template_grib_fd.close()
    count_data_points = int(ec.codes_get(gid, "numberOfDataPoints"))
    count_values = int(ec.codes_get(gid, "numberOfValues"))
    count_missing = int(ec.codes_get(gid, "numberOfMissing"))

    # This only supports messages without missing datapoints
    assert count_data_points == count_values
    assert count_missing == 0

    # Set common keys / data "pattern"
    ec.codes_set_string(gid, "type", "an")
    ec.codes_set_string(gid, "class", "ea")
    ec.codes_set_string(gid, "expver", "0001")
    ec.codes_set_string(gid, "stream", "oper")
    ec.codes_set_string(gid, "levtype", "sfc")
    ec.codes_set_values(gid, list(range(0, count_values)))

    dates = [20200101, 20200102, 20200103, 20200104]
    times = [0, 300, 600, 900, 1200, 1500, 1800, 2100]
    parameters = [167, 131, 132]

    messages = test_case_tmp / "test_data.grib"
    with open(messages, "wb") as out:
        for date, time, parameter in itertools.product(dates, times, parameters):
            ec.codes_set(gid, "date", date)
            ec.codes_set(gid, "time", time)
            ec.codes_set(gid, "paramId", parameter)
            ec.codes_write(gid, out)

    ec.codes_release(gid)
    return messages


@pytest.fixture(scope="function", autouse=True)
def fdb_config_path(
    data_path, test_case_tmp, build_grib_messages, doctest_namespace
) -> pathlib.Path:
    """
    Creates a FDB setup in this tests temp directory.
    Test FDB currently reads all grib files in `tests/data`
    This setup can be shared between tests as we will only read
    data from this FDB
    """
    schema_path_src = data_path / "schema"
    assert schema_path_src.is_file()
    schema_path = test_case_tmp / "schema"
    shutil.copy(schema_path_src, schema_path)

    db_store_path = test_case_tmp / "db_store"
    db_store_path.mkdir()
    fdb_config = {
        "type": "local",
        "engine": "toc",
        "schema": str(schema_path),
        "spaces": [
            {
                "handler": "Default",
                "roots": [
                    {"path": str(db_store_path)},
                ],
            }
        ],
    }
    fdb_config_str = yaml.dump(fdb_config)
    fdb_config_path = test_case_tmp / "fdb_config.yaml"
    fdb_config_path.write_text(fdb_config_str)
    fdb = pyfdb.FDB(fdb_config_str)
    fdb.archive(build_grib_messages.read_bytes())
    fdb.flush()
    doctest_namespace["fdb_config_path"] = fdb_config_path
    return fdb_config_path
