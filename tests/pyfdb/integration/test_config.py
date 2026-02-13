from pathlib import Path
import yaml
from pyfdb import FDB


def test_initialization_config_fixture(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path)

    assert fdb


def test_initialization_config_equality(read_only_fdb_setup):
    fdb_config_path: Path = read_only_fdb_setup

    assert fdb_config_path

    pyfdb_config_str = FDB(fdb_config_path.read_text())
    assert pyfdb_config_str

    pyfdb_config_path = FDB(fdb_config_path)
    assert pyfdb_config_path

    config_dict = yaml.safe_load(fdb_config_path.read_bytes())
    pyfdb_config_dict = FDB(config_dict)
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

    fdb = FDB(fdb_config_path, fdb_user_config_str)
    assert fdb

    system_config, user_config = fdb.config()

    assert system_config
    assert user_config["useSubToc"] is True


def test_initialization_config_system_and_user_printing(read_only_fdb_setup):
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

    fdb = FDB(fdb_config_path, fdb_user_config_str)
    assert fdb

    system_config, user_config = fdb.config()

    assert "db_store" in repr(system_config)
    assert "'useSubToc': True" in repr(user_config)


def test_initialization_config_system_and_user_round_trip(read_only_fdb_setup):
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

    fdb = FDB(fdb_config_path, fdb_user_config_str)
    assert fdb

    system_config, user_config = fdb.config()

    fdb2 = FDB(system_config, user_config)
    assert fdb2

    system_config_2, user_config_2 = fdb2.config()

    assert system_config == system_config_2
    assert user_config == user_config_2
