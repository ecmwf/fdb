# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import pytest

from pyfdb.pyfdb import FDB


def test_list_backward_compat_list(read_only_fdb_setup):
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
        "param": ["131", "132", "167"],
        "time": "1800",
    }

    elements = list(fdb.list(selection))

    for el in elements:
        print(el)

    assert len(elements) == 3


def test_list_backward_compat_list_of_list_str(read_only_fdb_setup):
    fdb = FDB(read_only_fdb_setup)
    # Next case should err, as this asks for a parameter "131/132/167" and
    # is wrapped in a list.
    selection_inconsistent = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "param": ["131/132/167"],
        "time": "1800",
    }

    with pytest.raises(RuntimeError, match="UserError"):
        _ = list(fdb.list(selection_inconsistent))


def test_list_backward_compat_str(read_only_fdb_setup):
    fdb = FDB(read_only_fdb_setup)
    selection_backward_comp = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101",
        "levtype": "sfc",
        "step": "0",
        "param": "131/132/167",
        "time": "1800",
    }

    elements = list(fdb.list(selection_backward_comp))

    for el in elements:
        print(el)

    assert len(elements) == 3


def test_list_backward_compat_to_list(read_only_fdb_setup):
    fdb = FDB(read_only_fdb_setup)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": ["20200101", "to", "20200103"],
        "levtype": "sfc",
        "step": "0",
        "param": ["131", "132", "167"],
        "time": "1800",
    }

    elements = list(fdb.list(selection))

    for el in elements:
        print(el)

    assert len(elements) == 9


def test_list_backward_compat_to_str(read_only_fdb_setup):
    fdb = FDB(read_only_fdb_setup)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101/to/20200103",
        "levtype": "sfc",
        "step": "0",
        "param": ["131", "132", "167"],
        "time": "1800",
    }

    elements = list(fdb.list(selection))

    for el in elements:
        print(el)

    assert len(elements) == 9


def test_list_backward_compat_list_of_str_list(read_only_fdb_setup):
    fdb = FDB(read_only_fdb_setup)

    # Next case should err, as this asks for a date "20200101/to/20200102" and
    # is wrapped in a list.
    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": ["20200101/to/20200103"],
        "levtype": "sfc",
        "step": "0",
        "param": ["131", "132", "167"],
        "time": "1800",
    }

    with pytest.raises(RuntimeError, match="UserError: Bad value"):
        _ = list(fdb.list(selection))


def test_list_backward_compat_to_by_list(read_only_fdb_setup):
    fdb = FDB(read_only_fdb_setup)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": ["20200101", "to", "20200104", "by", "1"],
        "levtype": "sfc",
        "step": "0",
        "param": ["131", "132", "167"],
        "time": "1800",
    }

    elements = list(fdb.list(selection))

    for el in elements:
        print(el)

    assert len(elements) == 12


def test_list_backward_compat_to_by_str(read_only_fdb_setup):
    fdb = FDB(read_only_fdb_setup)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "20200101/to/20200104/by/1",
        "levtype": "sfc",
        "step": "0",
        "param": ["131", "132", "167"],
        "time": "1800",
    }

    elements = list(fdb.list(selection))

    for el in elements:
        print(el)

    assert len(elements) == 12


def test_list_backward_compat_to_by_list_of_str(read_only_fdb_setup):
    fdb = FDB(read_only_fdb_setup)

    # Next case should err, as this asks for a date "20200101/to/20200102" and
    # is wrapped in a list.
    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": ["20200101/to/20200104/by/1"],
        "levtype": "sfc",
        "step": "0",
        "param": ["131", "132", "167"],
        "time": "1800",
    }

    with pytest.raises(RuntimeError, match="UserError: Bad value"):
        _ = list(fdb.list(selection))


def test_list_backward_compat_list_relative_dates_list(
    empty_fdb_setup, build_grib_messages_relative_dates
):
    fdb = FDB(empty_fdb_setup)

    with fdb:
        fdb.archive(build_grib_messages_relative_dates.read_bytes())

    elements = list(fdb.list({}))

    for el in elements:
        print(el)

    selection = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": ["-2", "-1", "0"],
        "levtype": "sfc",
        "step": "0",
        "param": ["131", "132", "167"],
        "time": "1800",
    }

    elements = list(fdb.list(selection))

    for el in elements:
        print(el)

    assert len(elements) == 9


def test_list_backward_compat_list_relative_dates_list_of_str(
    empty_fdb_setup, build_grib_messages_relative_dates
):
    fdb = FDB(empty_fdb_setup)

    with fdb:
        fdb.archive(build_grib_messages_relative_dates.read_bytes())

    # Next case should err, as this asks for a parameter "131/132/167" and
    # is wrapped in a list.
    selection_inconsistent = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": ["-2/-1/0"],
        "levtype": "sfc",
        "step": "0",
        "param": ["131/132/167"],
        "time": "1800",
    }

    with pytest.raises(RuntimeError, match="UserError"):
        _ = list(fdb.list(selection_inconsistent))


def test_list_backward_compat_list_relative_dates_str(
    empty_fdb_setup, build_grib_messages_relative_dates
):
    fdb = FDB(empty_fdb_setup)

    with fdb:
        fdb.archive(build_grib_messages_relative_dates.read_bytes())

    selection_backward_comp = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "-2/-1/0",
        "levtype": "sfc",
        "step": "0",
        "param": "131/132/167",
        "time": "1800",
    }

    elements = list(fdb.list(selection_backward_comp))

    for el in elements:
        print(el)

    assert len(elements) == 9


def test_list_backward_compat_list_relative_dates_to_str(
    empty_fdb_setup, build_grib_messages_relative_dates
):
    fdb = FDB(empty_fdb_setup)

    with fdb:
        fdb.archive(build_grib_messages_relative_dates.read_bytes())
    selection_backward_comp = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "-2/to/0",
        "levtype": "sfc",
        "step": "0",
        "param": "131/132/167",
        "time": "1800",
    }

    elements = list(fdb.list(selection_backward_comp))

    for el in elements:
        print(el)

    assert len(elements) == 9


def test_list_backward_compat_list_relative_dates_to_by_str(
    empty_fdb_setup, build_grib_messages_relative_dates
):
    fdb = FDB(empty_fdb_setup)

    with fdb:
        fdb.archive(build_grib_messages_relative_dates.read_bytes())
    selection_backward_comp = {
        "type": "an",
        "class": "ea",
        "domain": "g",
        "expver": "0001",
        "stream": "oper",
        "date": "-2/to/0/by/1",
        "levtype": "sfc",
        "step": "0",
        "param": "131/132/167",
        "time": "1800",
    }

    elements = list(fdb.list(selection_backward_comp))

    for el in elements:
        print(el)

    assert len(elements) == 9
