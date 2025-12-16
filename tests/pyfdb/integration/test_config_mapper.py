from pathlib import Path
import yaml
from pyfdb import PyFDB


def test_config_mapper_equality(read_only_fdb_setup):
    fdb_config_path: Path = read_only_fdb_setup

    assert fdb_config_path

    fdb_config_str = fdb_config_path.read_text()

    pyfdb_config_str = PyFDB(fdb_config_str)
    assert pyfdb_config_str

    pyfdb_config_path = PyFDB(fdb_config_path)
    assert pyfdb_config_path

    config_dict = yaml.safe_load(fdb_config_path.read_bytes())
    pyfdb_config_dict = PyFDB(config_dict)
    assert pyfdb_config_dict

    assert pyfdb_config_dict.print_config() == pyfdb_config_path.print_config()
    assert pyfdb_config_path.print_config() == pyfdb_config_str.print_config()


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

    pyfdb = PyFDB(fdb_config_path, fdb_user_config_str)
    assert pyfdb

    print(pyfdb.print_config())
    assert "useSubToc => true" in pyfdb.print_config()
