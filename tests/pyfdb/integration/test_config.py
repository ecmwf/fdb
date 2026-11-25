# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import pytest
import yaml

from pyfdb import FDB


def test_initialization_config_fixture(read_only_fdb_setup):
    fdb = FDB(read_only_fdb_setup)
    assert fdb


def test_initialization_config_equality(read_only_fdb_setup):
    pyfdb_config_str = FDB(read_only_fdb_setup.read_text())
    assert pyfdb_config_str

    pyfdb_config_path = FDB(read_only_fdb_setup)
    assert pyfdb_config_path

    config_dict = yaml.safe_load(read_only_fdb_setup.read_bytes())
    pyfdb_config_dict = FDB(config_dict)
    assert pyfdb_config_dict

    assert pyfdb_config_str.config() == pyfdb_config_path.config()
    assert pyfdb_config_path.config() == pyfdb_config_dict.config()


def test_initialization_config_different(read_only_fdb_setup):
    fdb_app_config_str_a = """
    type: local
    engine: toc
    useSubToc: true
    spaces:
    - roots:
      - path: "/a/path/is/something"
    """

    fdb_a = FDB(read_only_fdb_setup, fdb_app_config_str_a)

    fdb_app_config_str_b = """
    type: local
    engine: toc
    useSubToc: true
    spaces:
    - roots:
      - path: "/b/path/is/something"
    """

    fdb_b = FDB(read_only_fdb_setup, fdb_app_config_str_b)

    assert fdb_a.config() != fdb_b.config()


def test_initialization_config_system_and_app(read_only_fdb_setup):
    fdb_app_config_str = """
    type: local
    engine: toc
    useSubToc: true
    spaces:
    - roots:
      - path: "/a/path/is/something"
    """

    fdb = FDB(read_only_fdb_setup, fdb_app_config_str)
    assert fdb

    system_config, app_config = fdb.config()

    assert system_config
    assert app_config["useSubToc"] is True


def test_initialization_config_system_and_app_printing(read_only_fdb_setup):
    fdb_app_config_str = """
    type: local
    engine: toc
    useSubToc: true
    spaces:
    - roots:
      - path: "/a/path/is/something"
    """

    fdb = FDB(read_only_fdb_setup, fdb_app_config_str)
    assert fdb

    system_config, app_config = fdb.config()

    assert "db_store" in repr(system_config)
    assert "'useSubToc': True" in repr(app_config)


def test_initialization_config_system_and_app_round_trip(read_only_fdb_setup):
    fdb_app_config_str = """
    type: local
    engine: toc
    useSubToc: true
    spaces:
    - roots:
      - path: "/a/path/is/something"
    """

    fdb = FDB(read_only_fdb_setup, fdb_app_config_str)
    assert fdb

    system_config, app_config = fdb.config()

    fdb2 = FDB(system_config, app_config)
    assert fdb2

    system_config_2, app_config_2 = fdb2.config()
    assert system_config == system_config_2
    assert app_config == app_config_2


def test_initialization_user_config_deprecated(read_only_fdb_setup):
    with pytest.warns(DeprecationWarning, match="user_config"):
        fdb = FDB(read_only_fdb_setup, user_config={"useSubToc": True})

    _, app_config = fdb.config()
    assert app_config["useSubToc"] is True

    with pytest.raises(ValueError):
        FDB(read_only_fdb_setup, app_config={}, user_config={})
