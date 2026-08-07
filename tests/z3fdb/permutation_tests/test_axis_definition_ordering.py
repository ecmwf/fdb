# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

"""Tests verifying that specific axis definition orderings and chunking
configurations produce correct retrieval.

The MARS request uses canonical value ordering throughout; only the list of
AxisDefinitions passed to SimpleStoreBuilder varies. Scenarios covered:

- All axes individually chunked in canonical order (baseline)
- Non-chunked axes
- Swapped axis order
- Merged axes (date+time or time+date)
"""

import pytest
import zarr

from z3fdb import AxisDefinition, Chunking, ExtractorType, SimpleStoreBuilder

pytestmark = pytest.mark.offline

CANONICAL_REQUEST = {
    "type": "an",
    "class": "ea",
    "domain": "g",
    "expver": "0001",
    "stream": "oper",
    "levtype": "sfc",
    "step": 0,
    "date": "2020-01-01/to/2020-01-03",
    "param": [165, 166, 167],
    "time": "0/to/21/by/6",
}


def _open_array(store):
    return zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)


def _canonical_value(date, time, param, step=0):
    return step + param * 1 + time * 3 * 1 + date * 4 * 3 * 1


def test_canonical_axis_order_all_chunked(read_only_fdb_pattern_setup) -> None:
    """Canonical date/time/param/step axis order, all axes chunked individually."""
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        CANONICAL_REQUEST,
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

    for date in range(3):
        for time in range(4):
            for param in range(3):
                assert all(data[date, time, param, 0] == _canonical_value(date, time, param))


def test_canonical_axis_order_date_time_non_chunked(read_only_fdb_pattern_setup) -> None:
    """Date and time axes use Chunking.WHOLE_AXIS; param and step remain SINGLE_VALUE."""
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        CANONICAL_REQUEST,
        [
            AxisDefinition(["date"], Chunking.WHOLE_AXIS),
            AxisDefinition(["time"], Chunking.WHOLE_AXIS),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
            AxisDefinition(["step"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )
    data = _open_array(builder.build())
    assert data

    for date in range(3):
        for time in range(4):
            for param in range(3):
                assert all(data[date, time, param, 0] == _canonical_value(date, time, param))


def test_swapped_time_date_axes_non_chunked(read_only_fdb_pattern_setup) -> None:
    """Time axis listed before date axis; both non-chunked."""
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        CANONICAL_REQUEST,
        [
            AxisDefinition(["time"], Chunking.WHOLE_AXIS),
            AxisDefinition(["date"], Chunking.WHOLE_AXIS),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
            AxisDefinition(["step"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )
    data = _open_array(builder.build())
    assert data

    for date in range(3):
        for time in range(4):
            for param in range(3):
                assert all(data[time, date, param, 0] == _canonical_value(date, time, param))


def test_merged_date_time_axis_non_chunked(read_only_fdb_pattern_setup) -> None:
    """Date and time merged into a single non-chunked axis; datetime = time + date * 4."""
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        CANONICAL_REQUEST,
        [
            AxisDefinition(["date", "time"], Chunking.WHOLE_AXIS),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
            AxisDefinition(["step"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )
    data = _open_array(builder.build())
    assert data

    for date in range(3):
        for time in range(4):
            for param in range(3):
                datetime = time + date * 4
                assert all(data[datetime, param, 0] == _canonical_value(date, time, param))


def test_merged_time_date_axis_non_chunked_switched(read_only_fdb_pattern_setup) -> None:
    """Time before date in the merged axis; datetime = date + time * 3."""
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        CANONICAL_REQUEST,
        [
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
            AxisDefinition(["time", "date"], Chunking.WHOLE_AXIS),
            AxisDefinition(["step"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )
    data = _open_array(builder.build())
    assert data

    for date in range(3):
        for time in range(4):
            for param in range(3):
                datetime = date + time * 3
                assert all(data[param, datetime, 0] == _canonical_value(date, time, param))
