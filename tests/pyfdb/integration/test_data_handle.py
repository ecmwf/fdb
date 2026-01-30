import io
import pytest
from pyfdb.pyfdb import FDB

import shutil


def test_datahandle_repr(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "param": ["167", "165", "166"],
        "time": "1800",
    }

    data_handle = fdb.retrieve(selection)

    assert "Closed" in repr(data_handle)
    data_handle.open()
    assert "Opened" in repr(data_handle)


def test_datahandle_not_opened_before_read(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "param": ["167", "165", "166"],
        "time": "1800",
    }

    data_handle = fdb.retrieve(selection)

    assert data_handle

    with pytest.raises(
        RuntimeError,
        match="DataHandle: Read occured before the handle was opened. Must be opened first.",
    ):
        assert data_handle.read(4) == b"GRIB"


def test_datahandle_consecutive_read(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "param": ["167", "165", "166"],
        "time": "1800",
    }

    data_handle = fdb.retrieve(selection)

    assert data_handle

    # Check the GRIB header
    data_handle.open()
    assert data_handle.read(4) == b"GRIB"
    assert data_handle.read(4) != b"GRIB"


def test_datahandle_readinto(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "param": ["167", "165", "166"],
        "time": "1800",
    }

    with fdb.retrieve(selection) as data_handle:
        assert data_handle

        chunk_buf = bytearray(4)
        mv = memoryview(chunk_buf)
        data_handle.readinto(mv)

        assert chunk_buf == b"GRIB"
        assert data_handle.read(4) != b"GRIB"


def test_datahandle_read_all(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "param": ["167", "165", "166"],
        "time": "1800",
    }

    data_handle = fdb.retrieve(selection)

    # Check the GRIB header
    assert data_handle
    assert data_handle.readall()[0:4] == b"GRIB"
    data_handle.close()


def test_datahandle_cmp_read_read_all(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "param": ["167", "165", "166"],
        "time": "1800",
    }

    data_handle = fdb.retrieve(selection)
    data_handle_2 = fdb.retrieve(selection)

    assert data_handle
    assert data_handle_2

    data_handle_2.open()

    # Read all the data from data_handle
    data = data_handle.readall()
    data_2 = data_handle_2.read(len(data))

    assert data == data_2

    data_handle.open()
    print(data_handle.read(4))
    print(data_handle_2.read(4))


def test_datahandle_not_opened_before_read_context_manager(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "param": ["167", "165", "166"],
        "time": "1800",
    }

    with fdb.retrieve(selection) as data_handle:
        # Check the GRIB header
        assert data_handle
        assert data_handle.read(4) == b"GRIB"


def test_datahandle_not_opened_before_read_all_context_manager(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "param": ["167", "165", "166"],
        "time": "1800",
    }

    with fdb.retrieve(selection) as data_handle:
        assert data_handle
        assert data_handle.readall()


def test_datahandle_size(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "param": ["167", "165", "166"],
        "time": "1800",
    }

    with fdb.retrieve(selection) as data_handle:
        assert data_handle.size() == 10732


def test_datahandle_size_without_open(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "param": ["167", "165", "166"],
        "time": "1800",
    }

    data_handle = fdb.retrieve(selection)
    assert data_handle.size() == 10732


def test_datahandle_readinto_shutil_copyfileobj_cmp(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "param": ["167", "165", "166"],
        "time": "1800",
    }

    dst_read_into = io.BytesIO(b"")

    with fdb.retrieve(selection) as data_handle:
        assert data_handle
        print("Reading with shutil")
        shutil.copyfileobj(data_handle, dst_read_into)

        # reset position in file
        dst_read_into.seek(0)

    content_read_into = dst_read_into.read()

    with fdb.retrieve(selection) as data_handle:
        assert data_handle

        content_read_all = data_handle.readall()

        assert len(content_read_all) == len(content_read_into)
        assert content_read_all == content_read_into


def test_data_handle_readinto_attr(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "param": ["167", "165", "166"],
        "time": "1800",
    }

    with fdb.retrieve(selection) as data_handle:
        assert hasattr(data_handle, "readinto")


def test_data_handle_read_minus_one(read_only_fdb_setup):
    fdb_config_path = read_only_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "param": ["167", "165", "166"],
        "time": "1800",
    }

    content_read_minus_one = None
    content_read_all = None

    with fdb.retrieve(selection) as data_handle:
        content_read_minus_one = data_handle.read(-1)

    content_read_all = data_handle.readall()

    assert content_read_all
    assert content_read_minus_one
    assert len(content_read_minus_one) > 0
    assert len(content_read_all) > 0
    assert content_read_all == content_read_minus_one
