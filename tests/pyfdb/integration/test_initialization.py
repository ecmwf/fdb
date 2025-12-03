from pathlib import Path
import yaml
from pyfdb import PyFDB, Config
from pyfdb._internal import MarsRequest

import pytest


def test_initialization():
    pyfdb = PyFDB()
    assert pyfdb


def test_initialization_config():
    with pytest.raises(TypeError, match="missing 1 required positional argument"):
        Config()


def test_initialization_config_fixture(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    with fdb_config_path.open("r") as config_file:
        fdb_config = Config(config_file.read())
        pyfdb = PyFDB(fdb_config)

        assert pyfdb


def test_initialization_config_equality(read_only_fdb_setup):
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


def test_initialization_config_system_and_user(read_only_fdb_setup):
    fdb_config_path: Path = read_only_fdb_setup

    fdb_user_config_str = """
    type: local
    engine: toc
    useSubToc: true
    spaces:
    - roots:
      - path: "/a/path/is/something"
    """

    assert fdb_config_path

    fdb_sytem_config = Config(fdb_config_path.read_text())
    fdb_user_config = Config(fdb_user_config_str)

    pyfdb = PyFDB(fdb_sytem_config, fdb_user_config)
    assert pyfdb

    print(pyfdb.print_config())
    assert "useSubToc => true" in pyfdb.print_config()
