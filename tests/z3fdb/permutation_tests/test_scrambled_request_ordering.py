# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

"""Tests verifying that non-canonical value ordering in the MARS request does
not affect retrieval correctness.

The values for date, time, and param are listed in a scrambled order in the
request string.  The store must still map them to the correct zarr indices.
``_scrambled_value_at`` encodes the mapping from logical (date, time, param)
indices to the field value as injected by the fixture.
"""

import pytest
import zarr

from z3fdb import AxisDefinition, Chunking, ExtractorType, SimpleStoreBuilder

pytestmark = pytest.mark.offline

_MARS_BASE = {
    "type": "an",
    "class": "ea",
    "domain": "g",
    "expver": "0001",
    "stream": "oper",
    "levtype": "sfc",
    "step": 0,
}


def _open_array(store):
    return zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)


def _scrambled_value_at(param, time, date):
    """Expected field value when date/time/param are scrambled in the request.

    The fixture fills each field with a sequential index (0, 1, 2, …) in the
    order in which FDB stores them.  The permutations below express how the
    scrambled request order maps to that storage order.
    """
    date_perm = [2, 0, 1]  # 2020-01-03 → idx 0, 2020-01-01 → idx 1, 2020-01-02 → idx 2
    param_perm = [2, 0, 1]  # 167 → idx 0, 165 → idx 1, 166 → idx 2
    time_perm = [3, 0, 2, 1]
    return param_perm[param] * 1 + time_perm[time] * 3 * 1 + date_perm[date] * 4 * 3 * 1


def test_scrambled_request_canonical_axis_order(read_only_fdb_pattern_setup) -> None:
    """Scrambled date/param/time values in the request; axis definitions in canonical order."""
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        {
            **_MARS_BASE,
            "date": ["2020-01-03", "2020-01-01", "2020-01-02"],
            "param": [167, 165, 166],
            "time": [18, 0, 12, 6],
        },
        [
            AxisDefinition(["date"], Chunking.SINGLE_VALUE),
            AxisDefinition(["time"], Chunking.SINGLE_VALUE),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
            AxisDefinition(["step"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )
    data = _open_array(builder.build())
    assert data

    assert _scrambled_value_at(1, 1, 1) == 0  # first fixture entry → constant 0
    assert _scrambled_value_at(0, 0, 0) == 35  # last entry → 35

    for date in range(3):
        for time in range(4):
            for param in range(3):
                assert all(data[date, time, param, 0] == _scrambled_value_at(param, time, date))


def test_scrambled_request_swapped_axis_definitions(read_only_fdb_pattern_setup) -> None:
    """Scrambled request values combined with reordered AxisDefinitions.

    Axis definitions: time, step, param, date (instead of canonical date, time, param, step).
    """
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        {
            **_MARS_BASE,
            "time": [18, 0, 12, 6],
            "date": ["2020-01-03", "2020-01-01", "2020-01-02"],
            "param": [167, 165, 166],
        },
        [
            AxisDefinition(["time"], Chunking.SINGLE_VALUE),
            AxisDefinition(["step"], Chunking.SINGLE_VALUE),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
            AxisDefinition(["date"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )
    data = _open_array(builder.build())
    assert data

    assert _scrambled_value_at(1, 1, 1) == 0
    assert _scrambled_value_at(0, 0, 0) == 35

    for date in range(3):
        for time in range(4):
            for param in range(3):
                assert all(data[time, 0, param, date] == _scrambled_value_at(param, time, date))


def test_scrambled_request_all_axes_merged_non_chunked(read_only_fdb_pattern_setup) -> None:
    """All four axes merged into a single AxisDefinition with Chunking.WHOLE_AXIS.

    The zarr array is 1-D; elements are indexed by the linearised storage order.
    """
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        {
            **_MARS_BASE,
            "time": [18, 0, 12, 6],
            "date": ["2020-01-03", "2020-01-01", "2020-01-02"],
            "param": [167, 165, 166],
        },
        [AxisDefinition(["time", "step", "param", "date"], Chunking.WHOLE_AXIS)],
        ExtractorType.GRIB,
    )
    data = _open_array(builder.build())
    assert data

    assert _scrambled_value_at(1, 1, 1) == 0
    assert _scrambled_value_at(0, 0, 0) == 35

    def linearize(time, param, date):
        return date + param * 3 + time * 3 * 3

    for date in range(3):
        for time in range(4):
            for param in range(3):
                assert all(data[linearize(time, param, date)] == _scrambled_value_at(param, time, date))
