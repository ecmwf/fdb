# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""User-facing example: zarr dimension_names from AxisDefinition.name.

When an ``AxisDefinition`` is given a ``name``, that label appears in the
zarr array metadata as a ``dimension_names`` entry.  When no name is given,
one is auto-derived by joining the axis keys with ``"_"``.  The implicit
grid-point axis is always named ``"values"``.

Fixture data (from conftest.py build_pattern_grib_messages):
  dates  = [20200101, 20200102, 20200103]   (3 values)
  times  = [0, 600, 1200, 1800]             (4 values)
  params = [165, 166, 167]                  (3 values, surface)
"""

import logging
import pytest
import zarr

from z3fdb import (
    AxisDefinition,
    Chunking,
    ExtractorType,
    SimpleStoreBuilder,
)

pytestmark = [pytest.mark.offline, pytest.mark.zfdb_user_tests]

COMMON = {
    "type": "an",
    "class": "ea",
    "domain": "g",
    "expver": "0001",
    "stream": "oper",
    "levtype": "sfc",
    "step": 0,
    "date": ["2020-01-01", "2020-01-02", "2020-01-03"],
    "time": [0, 600, 1200, 1800],
    "param": [165, 166, 167],
}


def test_explicit_axis_names_appear_in_zarr_metadata(read_only_fdb_pattern_setup) -> None:
    """Explicit names on AxisDefinition are written into zarr dimension_names.

    With ``name="datetime"`` on the date+time axis and ``name="param"`` on
    the param axis, the resulting zarr array metadata must contain
    ``["datetime", "param", "values"]``.
    """
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        COMMON,
        [
            AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE, name="datetime"),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE, name="param"),
        ],
        ExtractorType.Grib(),
    )
    store = builder.build()

    arr = zarr.open_array(store)
    logging.debug(arr.metadata)
    assert arr.metadata.dimension_names == ("datetime", "param", "values")


def test_auto_derived_axis_names_from_keys(read_only_fdb_pattern_setup) -> None:
    """When no name is given, axis names are auto-derived from the MARS keys.

    A compound axis with ``keys=["date", "time"]`` becomes ``"date_time"``;
    a single-key axis with ``keys=["param"]`` becomes ``"param"``.
    The implicit grid-point axis is always ``"values"``.
    """
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        COMMON,
        [
            AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.Grib(),
    )
    store = builder.build()

    arr = zarr.open_array(store)
    assert arr.metadata.dimension_names == ("date_time", "param", "values")
