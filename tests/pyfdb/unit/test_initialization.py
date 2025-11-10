from pyfdb import PyFDB, Config
from pyfdb._internal import MarsRequest


def test_initialization():
    pyfdb = PyFDB()
    assert pyfdb


def test_initialization_config():
    config = Config()
    pyfdb = PyFDB(config)

    assert pyfdb


def test_initialization_metkit_request():
    request = MarsRequest()
    assert request is not None


def test_initialization_config_fixture(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    with fdb_config_path.open("r") as config_file:
        fdb_config = Config(config_file.read())
        pyfdb = PyFDB(fdb_config)

        assert pyfdb
