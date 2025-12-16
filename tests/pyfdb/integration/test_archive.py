import pytest
from pyfdb import FDB


STATIC_DICTIONARY = {
    "class": "rd",
    "date": "20191110",
    "domain": "g",
    "expver": "xxxx",
    "levelist": "300",
    "levtype": "pl",
    "param": "138",
    "step": "0",
    "stream": "oper",
    "time": "0000",
    "type": "an",
}


def assert_one_field(pyfdb: FDB):
    data_handle = pyfdb.retrieve(STATIC_DICTIONARY)

    assert data_handle

    list_iterator = pyfdb.list(STATIC_DICTIONARY)

    elements = []

    for el in list_iterator:
        print(el)
        elements.append(el)

    assert len(elements) == 1

    with pytest.raises(StopIteration) as _:
        next(list_iterator)


def test_archive_none(empty_fdb_setup, test_data_path):
    fdb_config_path = empty_fdb_setup

    assert fdb_config_path

    pyfdb = FDB(fdb_config_path)
    filename = test_data_path / "x138-300.grib"

    pyfdb.archive(open(filename, "rb").read())
    pyfdb.flush()

    assert_one_field(pyfdb)
