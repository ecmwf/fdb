import pytest
from pyfdb import FDB


def test_read_only_attributes_list_element(read_only_fdb_setup):
    fdb = FDB(read_only_fdb_setup)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "time": "1800",
    }

    list_iterator = fdb.list(selection, level=3)
    assert list_iterator

    elements = list(list_iterator)

    for i in range(len(elements)):
        with pytest.raises(AttributeError):
            elements[i].data_handle = None

    for i in range(len(elements)):
        with pytest.raises(AttributeError):
            elements[i].uri = None


def test_list_minimal(read_only_fdb_setup, test_logger):
    with FDB(read_only_fdb_setup) as fdb:
        selection = {
            "type": "an",
            "class": "ea",
            "domain": "g",
            "expver": "0001",
            "stream": "oper",
            "date": "20200101",
            "levtype": "sfc",
            "step": "0",
            "time": "1800",
        }
        test_logger.debug(f"Stringified selection:\n  {selection}")

        list_iterator = fdb.list(selection, level=1)
        assert list_iterator

        elements = []

        for el in list_iterator:
            test_logger.debug(el)
            elements.append(el)

        assert len(elements) == 1

        test_logger.debug("----------------------------------")

        selection = {
            "type": "an",
            "class": "ea",
            "domain": "g",
            "expver": "0001",
            "stream": "oper",
            "date": "20200101",
            "levtype": "sfc",
            "step": "0",
            "time": "1800",
        }

        list_iterator = fdb.list(selection, level=2)
        assert list_iterator

        elements = []

        for el in list_iterator:
            test_logger.debug(el)
            elements.append(el)

        assert len(elements) == 1

        test_logger.debug("----------------------------------")

        selection = {
            "type": "an",
            "class": "ea",
            "domain": "g",
            "expver": "0001",
            "stream": "oper",
            "date": "20200101",
            "levtype": "sfc",
            "step": "0",
            "time": "1800",
        }

        list_iterator = fdb.list(selection, level=3)
        assert list_iterator

        elements = []

        for el in list_iterator:
            test_logger.debug(el)
            test_logger.debug(el.uri)
            elements.append(el)

        assert len(elements) == 3


def test_list_deduplicate(read_only_fdb_setup, build_grib_messages):
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
        "param": "167",
        "time": "1800",
    }

    list_iterator = fdb.list(selection, include_masked=False, level=3)
    assert list_iterator

    elements = []

    for el in list_iterator:
        print(el)
        elements.append(el)

    assert len(elements) == 1

    print("Archiving duplicate data")
    fdb.archive(build_grib_messages.read_bytes())
    fdb.flush()

    list_iterator = fdb.list(selection, include_masked=True, level=3)
    assert list_iterator
    elements = []

    for el in list_iterator:
        print(el)
        elements.append(el)

    assert len(elements) == 2


def test_list_read_from_datahandle(read_only_fdb_setup):
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
        "param": "167/165/166",
        "time": "1800",
    }
    print(f"Stringified selection:\n  {selection}")

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "time": "1800",
    }

    list_iterator = fdb.list(selection, level=3)
    assert list_iterator

    elements = []

    for el in list_iterator:
        print(el)
        elements.append(el)

    assert len(elements) == 3

    for el in elements:
        data_handle = el.data_handle
        data_handle.open()
        assert data_handle.read(4) == b"GRIB"
        data_handle.close()
