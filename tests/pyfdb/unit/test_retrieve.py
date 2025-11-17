import pytest
from pyfdb import Config, PyFDB


def test_retrieve(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    with fdb_config_path.open("r") as config_file:
        fdb_config = Config(config_file.read())
        pyfdb = PyFDB(fdb_config)

        request = {
            "type": "an",
            "class": "ea",
            "domain": "g",
            "expver": "0001",
            "stream": "oper",
            "date": "20200101",
            "levtype": "sfc",
            "step": "0",
            "param": "167/165/166",
            "time": "1800",
        }

        data_handle = pyfdb.retrieve(request)

        assert data_handle
        data_handle.open()
        assert data_handle.read(4) == b"GRIB"
        data_handle.close()
