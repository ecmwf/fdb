# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import pytest

from pychunked_data_view.chunked_data_view import _mars_selection_to_string


# ---------------------------------------------------------------------------
# Realistic examples — ground truth strings taken verbatim from the
# add_part() calls that were replaced when migrating from str to dict.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "selection, expected",
    [
        (
            {
                "type": "an",
                "class": "ea",
                "domain": "g",
                "expver": "0001",
                "stream": "oper",
                "date": "2020-01-01/to/2020-01-04",
                "levtype": "sfc",
                "step": 0,
                "param": [167, 131, 132],
                "time": "0/to/21/by/3",
            },
            "type=an,class=ea,domain=g,expver=0001,stream=oper,"
            "date=2020-01-01/to/2020-01-04,levtype=sfc,step=0,"
            "param=167/131/132,time=0/to/21/by/3",
        ),
        (
            {
                "type": "an",
                "class": "ea",
                "domain": "g",
                "expver": "0001",
                "stream": "oper",
                "date": ["2020-01-01", "2020-01-02"],
                "levtype": "sfc",
                "step": 0,
                "param": [165, 166],
                "time": "0/to/21/by/3",
            },
            "type=an,class=ea,domain=g,expver=0001,stream=oper,"
            "date=2020-01-01/2020-01-02,levtype=sfc,step=0,"
            "param=165/166,time=0/to/21/by/3",
        ),
        (
            {
                "type": "an",
                "class": "ea",
                "domain": "g",
                "expver": "0001",
                "stream": "oper",
                "date": ["2020-01-01", "2020-01-02"],
                "levtype": "pl",
                "step": 0,
                "param": [131, 132],
                "levelist": [50, 100],
                "time": "0/to/21/by/3",
            },
            "type=an,class=ea,domain=g,expver=0001,stream=oper,"
            "date=2020-01-01/2020-01-02,levtype=pl,step=0,"
            "param=131/132,levelist=50/100,time=0/to/21/by/3",
        ),
        (
            {
                "type": "an",
                "class": "ea",
                "domain": "g",
                "expver": "0001",
                "stream": "oper",
                "date": ["2020-01-01", "2020-01-02", "2020-01-03"],
                "levtype": "sfc",
                "step": 0,
                "param": [165, 166, 167],
                "time": [0, 600, 1200, 1800],
            },
            "type=an,class=ea,domain=g,expver=0001,stream=oper,"
            "date=2020-01-01/2020-01-02/2020-01-03,levtype=sfc,step=0,"
            "param=165/166/167,time=0/600/1200/1800",
        ),
        (
            {
                "type": "an",
                "class": "ea",
                "domain": "g",
                "expver": "0001",
                "stream": "oper",
                "date": ["2020-01-01", "2020-01-02", "2020-01-03"],
                "levtype": "pl",
                "step": 0,
                "param": [131, 132, 133],
                "levelist": [50, 100, 150],
                "time": [0, 600, 1200, 1800],
            },
            "type=an,class=ea,domain=g,expver=0001,stream=oper,"
            "date=2020-01-01/2020-01-02/2020-01-03,levtype=pl,step=0,"
            "param=131/132/133,levelist=50/100/150,time=0/600/1200/1800",
        ),
    ],
)
def test_mars_selection_to_string_realistic_examples(selection, expected):
    assert _mars_selection_to_string(selection) == expected


# ---------------------------------------------------------------------------
# Value-type unit tests
# ---------------------------------------------------------------------------


def test_scalar_string_value():
    assert _mars_selection_to_string({"class": "ea"}) == "class=ea"


def test_scalar_int_value():
    assert _mars_selection_to_string({"step": 0}) == "step=0"


def test_scalar_float_value():
    assert _mars_selection_to_string({"threshold": 1.5}) == "threshold=1.5"


def test_list_of_ints_joined_with_slash():
    assert _mars_selection_to_string({"param": [167, 131, 132]}) == "param=167/131/132"


def test_list_of_various_types_joined_with_slash():
    assert (
        _mars_selection_to_string({"param": [167, 131.0, "132"]})
        == "param=167/131.0/132"
    )


def test_list_of_strings_joined_with_slash():
    assert (
        _mars_selection_to_string({"date": ["2020-01-01", "2020-01-02"]})
        == "date=2020-01-01/2020-01-02"
    )


def test_string_range_expression_passed_through():
    # MARS range syntax must survive unchanged — the string branch handles this.
    assert _mars_selection_to_string({"time": "0/to/21/by/3"}) == "time=0/to/21/by/3"


def test_multiple_keys_joined_with_comma():
    assert (
        _mars_selection_to_string({"levtype": "sfc", "step": 0}) == "levtype=sfc,step=0"
    )


def test_leading_zero_preserved_in_string_value():
    # expver "0001" must not be coerced to integer 1.
    assert _mars_selection_to_string({"expver": "0001"}) == "expver=0001"
