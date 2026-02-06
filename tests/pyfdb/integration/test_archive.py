import pytest
from pyfdb import FDB
from pyfdb.pyfdb_type import Identifier


def assert_one_field(pyfdb: FDB):
    selection = {
        "class": "rd",
        "expver": "xxxx",
        "stream": "oper",
        "date": "20191110",
        "time": "0000",
        "domain": "g",
        "type": "an",
        "levtype": "pl",
        "step": "0",
        "levelist": "300",
        "param": "138",
    }

    data_handle = pyfdb.retrieve(selection)

    assert data_handle

    list_iterator = pyfdb.list(selection)

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

    fdb = FDB(fdb_config_path)
    filename = test_data_path / "x138-300.grib"

    fdb.archive(open(filename, "rb").read())
    fdb.flush()

    assert_one_field(fdb)


def test_archive_abitrary_data(empty_fdb_setup):
    fdb_config_path = empty_fdb_setup

    assert fdb_config_path

    selection = {
        "class": "rd",
        "expver": "arbi",
        "stream": "oper",
        "date": "20191110",
        "time": "0000",
        "domain": "g",
        "type": "an",
        "levtype": "pl",
        "step": "0",
        "levelist": "300",
        "param": "138",
    }

    with FDB(fdb_config_path) as fdb:
        fdb.archive(
            data=b"binary-data",
            identifier=Identifier(selection),
        )

    with fdb.retrieve(selection) as data_handle:
        file_content = data_handle.readall()
        assert file_content == b"binary-data"


key_values = [
    ("stream", "oper/rd"),
    ("stream", ["oper", "rd"]),
]


@pytest.mark.parametrize("wrong_key_value", key_values)
def test_archive_abitrary_data_wrong_identifier(empty_fdb_setup, wrong_key_value):
    fdb_config_path = empty_fdb_setup

    assert fdb_config_path

    selection = {
        "class": "rd",
        "expver": "arbi",
        wrong_key_value[0]: wrong_key_value[1],
        "date": "20191110",
        "time": "0000",
        "domain": "g",
        "type": "an",
        "levtype": "pl",
        "step": "0",
        "levelist": "300",
        "param": "138",
    }

    with pytest.raises(ValueError):
        with FDB(fdb_config_path) as fdb:
            fdb.archive(
                data=b"binary-data",
                identifier=Identifier(selection),
            )


def test_archive_round_trip(empty_fdb_setup, test_data_path):
    fdb_config_path = empty_fdb_setup
    assert fdb_config_path

    fdb = FDB(fdb_config_path)

    filename = test_data_path / "x138-300.grib"
    file_content = open(filename, "rb").read()
    fdb.archive(file_content)
    fdb.flush()

    # Those are the FDB keys of the GRIB file given above
    selection = {
        "class": "rd",
        "expver": "xxxx",
        "stream": "oper",
        "date": "20191110",
        "time": "0000",
        "domain": "g",
        "type": "an",
        "levtype": "pl",
        "step": "0",
        "levelist": "300",
        "param": "138",
    }

    with fdb.retrieve(selection) as data_handle:
        reread_file_content = data_handle.readall()
        assert file_content == reread_file_content

    list_iterator = fdb.list({})

    assert len(list(list_iterator)) == 1
