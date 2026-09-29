# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

from pathlib import Path

import pytest

from pyfdb import FDB


def assert_one_field(pyfdb: FDB, test_logger):
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

    with pyfdb.retrieve(selection) as data_handle:
        assert data_handle

    list_iterator = pyfdb.list(selection)
    elements = list(list_iterator)

    for el in elements:
        test_logger.info(el)

    assert len(elements) == 1

    with pytest.raises(StopIteration) as _:
        next(list_iterator)


def test_archive_none(empty_fdb_setup, test_data_path, test_logger):
    fdb_config_path = empty_fdb_setup

    assert fdb_config_path

    fdb = FDB(fdb_config_path)
    filename: Path = test_data_path / "x138-300.grib"

    fdb.archive(filename.read_bytes())
    fdb.flush()

    assert_one_field(fdb, test_logger)


def test_archive_abitrary_data(empty_fdb_setup, test_logger):
    fdb_config_path = empty_fdb_setup

    assert fdb_config_path

    selection = {
        "class": "rd",
        "expver": "arb0",
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

    test_logger.debug("-----------ARCHIVE-------------------")

    with FDB(fdb_config_path) as fdb:
        fdb.archive(
            data=b"binary-data",
            identifier=selection,
        )

    test_logger.debug("-----------RETRIEVE-------------------")

    data_handle = fdb.retrieve(selection)
    file_content = data_handle.readall()
    assert file_content == b"binary-data"

    test_logger.debug("-----------CLOSE-------------------")


key_values = [
    ("stream", "oper/rd"),
    ("stream", ["oper", "rd"]),
]


@pytest.mark.parametrize("wrong_key_value", key_values)
def test_archive_abitrary_data_wrong_identifier(empty_fdb_setup, wrong_key_value):
    selection = {
        "class": "rd",
        "expver": "arb1",
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

    with pytest.raises(ValueError), FDB(empty_fdb_setup) as fdb:
        fdb.archive(
            data=b"binary-data",
            identifier=selection,
        )


def test_archive_round_trip(empty_fdb_setup, test_data_path, test_logger):
    fdb = FDB(empty_fdb_setup)

    filename = test_data_path / "x138-300.grib"
    file_content = filename.read_bytes()

    test_logger.debug("-----------ARCHIVE-------------------")
    test_logger.debug(f"Read {len(file_content)} bytes")
    fdb.archive(file_content)
    fdb.flush()

    test_logger.debug("-----------RETRIEVE-------------------")

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
        test_logger.debug(f"Original size: {len(file_content)}")
        test_logger.debug(f"Reread size: {len(reread_file_content)}")
        assert file_content == reread_file_content

    list_iterator = fdb.list({})

    assert len(list(list_iterator)) == 1
