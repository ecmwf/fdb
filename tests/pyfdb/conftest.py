# (C) Copyright 2011- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import logging
import os
import pathlib
from typing import Generator

import psutil
import pytest
import itertools
import shutil
import yaml
import git

import eccodes as ec

import pyfdb

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


@pytest.fixture
def test_logger():
    return logger


def print_open_files_psutil():
    """
    Prints open files for a process (default: current).
    Requires psutil. On some OSes, elevated privileges may be required
    to inspect other processes.
    """
    pid = os.getpid()
    proc = psutil.Process(pid)
    try:
        files = proc.open_files()
    except psutil.AccessDenied:
        logger.debug(f"Access denied for pid {pid}. Try running with higher privileges.")
        return
    except psutil.NoSuchProcess:
        logger.debug(f"No such process: {pid}")
        return

    if not files:
        logger.debug(f"No open files for pid {pid}.")
        return

    logger.debug(f"Open files for pid {pid} | Total={proc.num_fds()}:")
    for f in files:
        logger.debug(f" - path={f.path}  |  fd={getattr(f, 'fd', '?')}")


@pytest.fixture(autouse=True)
def run_around_tests():
    # Code that will run before your test, for example:
    print_open_files_psutil()
    yield

    print_open_files_psutil()
    # Code that will run after your test, for example:


@pytest.fixture(scope="function")
def data_path() -> pathlib.Path:
    """
    Provides path to test data
    """
    path = pathlib.Path(__file__).resolve().parent / "data"
    assert path.exists()
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


@pytest.fixture(scope="session")
def session_tmp(tmp_path_factory) -> Generator[pathlib.Path, None, None]:
    tmp_dir = tmp_path_factory.mktemp("session_data")
    yield tmp_dir
    shutil.rmtree(str(tmp_dir))


@pytest.fixture(scope="function")
def function_tmp(tmp_path_factory) -> Generator[pathlib.Path, None, None]:
    tmp_function_dir = tmp_path_factory.mktemp("test_data", numbered=True)
    yield tmp_function_dir
    shutil.rmtree(str(tmp_function_dir))


@pytest.fixture(scope="function")
def build_grib_messages(data_path, session_tmp) -> pathlib.Path:
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

    messages = session_tmp / "test_data.grib"
    with open(messages, "wb") as out:
        for date, time, parameter in itertools.product(dates, times, parameters):
            ec.codes_set(gid, "date", date)
            ec.codes_set(gid, "time", time)
            ec.codes_set(gid, "paramId", parameter)
            ec.codes_write(gid, out)

    ec.codes_release(gid)
    return messages


@pytest.fixture(scope="function", autouse=False)
def empty_fdb_setup(data_path, session_tmp, function_tmp) -> pathlib.Path:
    """
    Creates a FDB setup in this tests temp directory.
    This setup can be shared between tests
    """
    schema_path_src = data_path / "schema"
    assert schema_path_src.is_file()
    schema_path = session_tmp / "schema"
    shutil.copy(schema_path_src, schema_path)

    db_store_path = session_tmp / function_tmp / "db_store"
    db_store_path.mkdir()
    fdb_config = dict(
        type="local",
        engine="toc",
        schema=str(schema_path),
        spaces=[
            dict(
                handler="Default",
                roots=[
                    {"path": str(db_store_path)},
                ],
            )
        ],
    )
    fdb_config_str = yaml.dump(fdb_config)
    fdb_config_path = session_tmp / "fdb_config.yaml"
    fdb_config_path.write_text(fdb_config_str)
    return fdb_config_path


@pytest.fixture(scope="function", autouse=False)
def read_only_fdb_setup(empty_fdb_setup, build_grib_messages) -> pathlib.Path:
    """
    Creates a FDB setup in this tests temp directory.
    Test FDB currently reads all grib files in `tests/data`
    This setup can be shared between tests as we will only read
    data from this FDB
    """

    logger.debug("***********READ ONLY FDB SETUP*******************")
    with pyfdb.FDB(empty_fdb_setup) as fdb:
        fdb.archive(build_grib_messages.read_bytes())
        fdb.flush()

    print_open_files_psutil()
    return empty_fdb_setup


@pytest.fixture(scope="function", autouse=False)
def read_write_fdb_setup(empty_fdb_setup, build_grib_messages) -> pathlib.Path:
    """
    Creates a FDB setup in this tests temp directory.
    Test FDB currently reads all grib files in `tests/data`
    This setup can be shared between tests as we will only read
    data from this FDB
    """
    with pyfdb.FDB(empty_fdb_setup) as fdb:
        fdb.archive(build_grib_messages.read_bytes())
        fdb.flush()
    return empty_fdb_setup
