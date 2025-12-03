import pytest
from pyfdb import PyFDB
from pyfdb.pyfdb import Config, FDBToolRequest


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


def assert_one_field(pyfdb: PyFDB):
    data_handle = pyfdb.retrieve(STATIC_DICTIONARY)

    assert data_handle

    request = FDBToolRequest(STATIC_DICTIONARY)
    list_iterator = pyfdb.list(request)

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

    with fdb_config_path.open("r") as config_file:
        fdb_config = Config(config_file.read())
        pyfdb = PyFDB(fdb_config)

        filename = test_data_path / "x138-300.grib"

        pyfdb.archive(open(filename, "rb").read())
        pyfdb.flush()

    assert_one_field(pyfdb)
