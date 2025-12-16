from pathlib import Path

import pytest
import yaml

from pyfdb import Config, PyFDB


def test_initialization():
    pyfdb = PyFDB()
    assert pyfdb


def test_fdb_config():
    with pytest.raises(
        TypeError,
        match="missing 1 required positional argument: 'config'",
    ):
        config = Config()


def test_fdb_config_wrong_type():
    with pytest.raises(
        RuntimeError, match="Config: Unknown config type, must be str, dict or Path"
    ):
        config = Config(0)
        pyfdb = PyFDB(config)

        assert pyfdb


def test_fdb_config_fixture(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    with fdb_config_path.open("r") as config_file:
        fdb_config = Config(config_file.read())
        pyfdb = PyFDB(fdb_config)

        assert pyfdb


def test_fdb_config_equality(read_only_fdb_setup):
    fdb_config_path: Path = read_only_fdb_setup

    assert fdb_config_path

    fdb_config_str = Config(fdb_config_path.read_text())
    pyfdb_config_str = PyFDB(fdb_config_str)
    assert pyfdb_config_str

    pyfdb_config_path = PyFDB(fdb_config_path)
    assert pyfdb_config_path

    config_dict = yaml.safe_load(fdb_config_path.read_bytes())
    pyfdb_config_dict = PyFDB(config_dict)
    assert pyfdb_config_dict

    print(pyfdb_config_str)
    print(pyfdb_config_path)
    print(pyfdb_config_dict)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
    }

    elements_str = list(pyfdb_config_str.status(selection))
    elements_path = list(pyfdb_config_path.status(selection))
    elements_dict = list(pyfdb_config_dict.status(selection))

    print(elements_str)
    print(elements_dict)
    print(elements_str)

    assert all(
        [x == y == z for (x, y, z) in zip(elements_str, elements_path, elements_dict)]
    )


def test_fdb_user_config(read_only_fdb_setup):
    fdb_config_path: Path = read_only_fdb_setup

    assert fdb_config_path

    fdb_config_str = Config(fdb_config_path.read_text())

    user_config_str = r"""---
        type: local
        engine: toc
        useSubToc: true
        spaces:
        - roots:
          - path: "/a/path/is/something"
    """
    user_config = Config(user_config_str)

    pyfdb = PyFDB(fdb_config_str, user_config)
    assert pyfdb

    print("Check for user config propagation:")
    print(pyfdb.print_config())
    assert "useSubToc => true" in pyfdb.print_config()

    pyfdb_no_user_config = PyFDB(fdb_config_str)
    print(pyfdb_no_user_config.print_config())
    print("Check for empty user config:")
    assert "root={}" in pyfdb_no_user_config.print_config()


def test_fdb_user_config_no_config_constructor(read_only_fdb_setup):
    fdb_config_path: Path = read_only_fdb_setup

    assert fdb_config_path

    fdb_config_str = Config(fdb_config_path.read_text())

    user_config_str = r"""---
        type: local
        engine: toc
        useSubToc: true
        spaces:
        - roots:
          - path: "/a/path/is/something"
    """
    pyfdb = PyFDB(fdb_config_str, user_config_str)
    assert pyfdb

    print("Check for user config propagation:")
    print(pyfdb.print_config())
    assert "useSubToc => true" in pyfdb.print_config()

    pyfdb_no_user_config = PyFDB(fdb_config_str)
    print(pyfdb_no_user_config.print_config())
    print("Check for empty user config:")
    assert "root={}" in pyfdb_no_user_config.print_config()


def test_fdb_print():
    pyfdb = PyFDB()

    assert pyfdb
    print(pyfdb)
