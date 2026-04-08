# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import pytest

from pyfdb import FDB


def test_retrieve(read_only_fdb_setup):
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

    data_handle = fdb.retrieve(selection)

    assert data_handle
    data_handle.open()
    assert data_handle.read(4) == b"GRIB"
    data_handle.close()


def test_retrieve_wildcard(read_only_fdb_setup):
    fdb = FDB(read_only_fdb_setup)

    with pytest.raises(TypeError):
        _ = fdb.retrieve({})


def test_retrieve_context_manager(read_only_fdb_setup):
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

    with fdb.retrieve(selection) as data_handle:
        assert data_handle
        assert data_handle.read(4) == b"GRIB"
