from pyfdb import FDB
from pyfdb.pyfdb_iterator import ListElement


def test_inspect(read_only_fdb_setup):
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
        "param": "131",
        "time": "1800",
    }

    list_iterator = fdb.inspect(selection)

    assert list_iterator

    elements: list[ListElement] = []

    for el in list_iterator:
        elements.append(el)
        print(el)

    assert len(elements) == 1

    for el in elements:
        data_handle = el.data_handle
        assert data_handle
        data_handle.open()
        assert data_handle.read(4) == b"GRIB"
        data_handle.close()


def test_inspect_multiple_values(read_only_fdb_setup):
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
        "param": "131/167",
        "time": "1800",
    }

    list_iterator = fdb.inspect(selection)

    assert list_iterator

    elements = []

    for el in list_iterator:
        elements.append(el)
        print(el)

    assert len(elements) == 2

    for el in elements:
        data_handle = el.data_handle
        assert data_handle
        data_handle.open()
        assert data_handle.read(4) == b"GRIB"
        data_handle.close()
