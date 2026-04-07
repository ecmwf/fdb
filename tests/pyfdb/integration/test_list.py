# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import pytest
from pyfdb import FDB


def test_list_empty_fdb_list_non_existing(empty_fdb_setup):
    fdb = FDB(empty_fdb_setup)

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
    assert len(elements) == 0


def test_list_empty_fdb_list_all(empty_fdb_setup):
    fdb = FDB(empty_fdb_setup)

    list_iterator = fdb.list({}, level=3)
    assert list_iterator

    elements = list(list_iterator)
    assert len(elements) == 0


def test_list_partial(read_only_fdb_setup):
    fdb = FDB(read_only_fdb_setup)

    selection = {"type": "an"}

    list_iterator = fdb.list(selection, level=1)
    assert list_iterator

    elements = list(list_iterator)
    assert len(elements) == 32


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

    for el in elements:
        with pytest.raises(AttributeError):
            el.data_handle = None

    for el in elements:
        with pytest.raises(AttributeError):
            el.uri = None


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
            test_logger.debug(f"Has Location: {el.has_location()}")

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
            test_logger.debug(f"Has Location: {el.has_location()}")

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
            test_logger.debug(f"Has Location: {el.has_location()}")
            test_logger.debug(f"Has URI: {el.uri}")
            test_logger.debug(f"Has Offset: {el.offset}")
            test_logger.debug(f"Has Length: {el.length}")
            elements.append(el)

        assert len(elements) == 3


def test_list_deduplicate(read_only_fdb_setup, build_grib_messages):
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
        "param": ["167", "165", "166"],
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
