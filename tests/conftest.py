# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

"""Top-level pytest configuration."""

import os
import pathlib
import shutil
import itertools

import eccodes as ec
import pyfdb
import pytest
import yaml


def create_fdb(root: pathlib.Path, schema_src: pathlib.Path) -> pathlib.Path:
    """Create a new FDB under 'root' using the provided schema.

    This helper function is supposed to be used to build specific test FDB setups.

    Args:
        root (pathlib.Path): Directory that will hold the resulting ``.fdb``.
        schema_src (pathlib.Path): Path to a text file containing the schema definition.

    Returns:
        pathlib.Path: Absolute path to the created ``fdb_config.yaml`` file.

    Asserts:
        - ``root`` needs to already exist.
        - ``schema`` file exists
    """
    assert root.is_dir()
    assert schema_src.is_file()
    schema_path = root / "schema"
    shutil.copy(schema_src, schema_path)

    db_store_path = root / "db_store"
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
    fdb_config_path = root / "fdb_config.yaml"
    fdb_config_path.write_text(fdb_config_str)
    return fdb_config_path


def populate_fdb(config: pathlib.Path, messages: list[pathlib.Path]):
    """Import 'messages' into fdb pointed to by 'config'

    This helper function is supposed to be used to build specific test FDB setups.

    Args:
        config (pathlib.Path): FDB database to use.
        messages (list[pathlib.Path]): List of files with messages to ingest.

    Asserts:
        - FDB config file needs to exist
        - All message files need to exist

    """
    assert config.is_file()
    fdb = pyfdb.FDB(config.read_text())
    for message in messages:
        assert message.is_file()
        fdb.archive(message.read_bytes())
    fdb.flush()


@pytest.fixture(scope="session")
def data_path() -> pathlib.Path:
    """
    Provides path to test data
    """
    path = pathlib.Path(__file__).resolve().parent / "data"
    assert path.exists()
    return path


@pytest.fixture(scope="session")
def session_tmp(tmp_path_factory) -> pathlib.Path:
    return tmp_path_factory.mktemp("session_data")


@pytest.fixture(scope="session")
def build_example_recipe_grib_messages(data_path, session_tmp) -> pathlib.Path:
    """
    Build messages that span the data required to match mars requests in
    'data/anemoi-recipes/example.yaml'
    """
    tmp = session_tmp / "build_example_recipe_grib_messages"
    tmp.mkdir()
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
    ec.codes_set_values(gid, list(range(0, count_values)))
    #ec.codes_set_string(gid, "bitsPerValue", "66")

    dates = [20200101, 20200102]
    times = [0, 6, 12, 18]
    # 10u/10v
    parameters_sfc = [165, 166]

    # u/v
    parameters_pl = [131, 132]
    levels = [50, 100]

    messages = tmp / "test_data.grib"
    with open(messages, "wb") as out:
        ec.codes_set_string(gid, "levtype", "sfc")
        for date, time, parameter in itertools.product(dates, times, parameters_sfc):
            ec.codes_set(gid, "date", date)
            ec.codes_set(gid, "time", time)
            ec.codes_set(gid, "paramId", parameter)
            ec.codes_write(gid, out)

        ec.codes_set_string(gid, "levtype", "pl")
        for date, time, parameter, level in itertools.product(
            dates, times, parameters_pl, levels
        ):
            ec.codes_set(gid, "date", date)
            ec.codes_set(gid, "time", time)
            ec.codes_set(gid, "paramId", parameter)
            ec.codes_set(gid, "level", level)
            ec.codes_write(gid, out)

    ec.codes_release(gid)
    return messages


@pytest.fixture(scope="session")
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
    times = [0, 3, 6, 9, 12, 15, 18, 21]
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


@pytest.fixture(scope="session", autouse=False)
def read_only_fdb_setup_for_anemoi_recipe_example(
    data_path, session_tmp, build_example_recipe_grib_messages
) -> pathlib.Path:
    """
    Creates a FDB setup in this tests temp directory.
    Test FDB currently reads all grib files in `tests/data`
    This setup can be shared between tests as we will only read
    data from this FDB
    """
    fdb_root = session_tmp / "anemoi-recipes-example-fdb"
    fdb_root.mkdir()
    schema_path_src = data_path / "schema"
    cfg = create_fdb(fdb_root, schema_path_src)
    populate_fdb(cfg, [build_example_recipe_grib_messages])
    return cfg


@pytest.fixture(scope="session", autouse=False)
def read_only_fdb_setup(data_path, session_tmp, build_grib_messages) -> pathlib.Path:
    """
    Creates a FDB setup in this tests temp directory.
    Test FDB currently reads all grib files in `tests/data`
    This setup can be shared between tests as we will only read
    data from this FDB
    """
    schema_path_src = data_path / "schema"
    assert schema_path_src.is_file()
    schema_path = session_tmp / "schema"
    shutil.copy(schema_path_src, schema_path)

    db_store_path = session_tmp / "db_store"
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
    fdb = pyfdb.FDB(fdb_config_str)
    fdb.archive(build_grib_messages.read_bytes())
    fdb.flush()
    return fdb_config_path
