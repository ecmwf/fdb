import pytest
from pyfdb.pyfdb import PyFDB
from pyfdb.pyfdb_type import Config


def test_datahandle_repr(read_only_fdb_setup):
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

        assert "Closed" in repr(data_handle)
        data_handle.open()
        assert "Opened" in repr(data_handle)


def test_datahandle_not_opened_before_read(read_only_fdb_setup):
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

        # Check the GRIB header
        assert data_handle

    with pytest.raises(
        RuntimeError,
        match="DataHandle: Read occured before the handle was opened. Must be opened first.",
    ):
        assert data_handle.read(4) == b"GRIB"


def test_datahandle_consecutive_read(read_only_fdb_setup):
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

        # Check the GRIB header
        assert data_handle

    data_handle.open()
    assert data_handle.read(4) == b"GRIB"
    assert data_handle.read(4) != b"GRIB"


def test_datahandle_read_all(read_only_fdb_setup):
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

        # Check the GRIB header
        assert data_handle
        assert data_handle.read_all()[0:4] == b"GRIB"
        data_handle.close()


def test_datahandle_cmp_read_read_all(read_only_fdb_setup):
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
        data_handle_2 = pyfdb.retrieve(request)

        # Check the GRIB header
        assert data_handle
        assert data_handle_2

        data_handle_2.open()

        # Read all the data from data_handle
        data = data_handle.read_all()
        data_2 = data_handle_2.read(len(data))

        assert data == data_2

        data_handle.open()
        print(data_handle.read(4))
        print(data_handle_2.read(4))


def test_datahandle_not_opened_before_read_context_manager(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    pyfdb = PyFDB(fdb_config_path.read_text())

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

    with pyfdb.retrieve(request) as data_handle:
        # Check the GRIB header
        assert data_handle
        assert data_handle.read(4) == b"GRIB"
