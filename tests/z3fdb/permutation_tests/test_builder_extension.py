# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Tests that verify extension works for SingleValueChunking and FixedSizeChunking
when parts share the same axis structure but span non-overlapping value ranges.

These tests complement the existing test_store_multiple_parts.py which only tests
extension along axes that differ in key-structure between parts (e.g. SFC ["param"]
vs PL ["param","levelist"]).  Here we split the *same* MARS key's value range across
multiple parts — e.g. dates=[2020-01-01,2020-01-02] in Part 1 and date=[2020-01-03]
in Part 2.

Fixture data (from conftest.py build_pattern_grib_messages):
  dates      = [20200101, 20200102, 20200103]   (3 values, 0-based index d)
  times      = [0, 600, 1200, 1800]             (4 values, 0-based index t)
  params_sfc = [165, 166, 167]                  (3 values, 0-based index p)
  params_pl  = [131, 132, 133]                  (3 values, 0-based index p)
  levels     = [50, 100, 150]                   (3 values, 0-based index l)

Field-value formulas (all indices 0-based):
  sfc_value(d, t, p)    = d*12 + t*3 + p
  pl_value(d, t, p, l)  = 36 + d*36 + t*9 + p*3 + l
"""

import numpy as np
import pytest
import zarr

from z3fdb import (
    AxisDefinition,
    Chunking,
    ExtractorType,
    SimpleStoreBuilder,
)

pytestmark = pytest.mark.offline

COMMON = {
    "type": "an",
    "class": "ea",
    "domain": "g",
    "expver": "0001",
    "stream": "oper",
    "step": 0,
}


# SingleValueChunking: two date batches, extend on date axis
def test_single_value_chunking_date_extension(
    read_only_fdb_pattern_setup,
) -> None:
    """Extension along the date axis (axis 0) using SingleValueChunking.

    Both parts share the same axis structure:
      Axis 0: ["date"]  — SingleValueChunking (chunk size 1)
      Axis 1: ["time"]  — SingleValueChunking (chunk size 1)
      Axis 2: ["param"] — SingleValueChunking (chunk size 1)

    Part 1 covers dates 2020-01-01 and 2020-01-02 (d=0,1 → 2 chunks on axis 0).
    Part 2 covers date  2020-01-03               (d=2   → 1 chunk on axis 0).

    After extend_on_axis(0): 3 date chunks × 4 time chunks × 3 param chunks.

    Array layout:
      data[d, t, p] = sfc_value(d, t, p) = d*12 + t*3 + p
    """
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    _axes = [
        AxisDefinition(["date"], Chunking.SINGLE_VALUE),
        AxisDefinition(["time"], Chunking.SINGLE_VALUE),
        AxisDefinition(["param"], Chunking.SINGLE_VALUE),
    ]

    builder.add_part(
        {
            **COMMON,
            "levtype": "sfc",
            "date": ["2020-01-01", "2020-01-02"],
            "time": [0, 600, 1200, 1800],
            "param": [165, 166, 167],
        },
        _axes,
        ExtractorType.Grib(),
    )
    builder.add_part(
        {
            **COMMON,
            "levtype": "sfc",
            "date": ["2020-01-03"],
            "time": [0, 600, 1200, 1800],
            "param": [165, 166, 167],
        },
        _axes,
        ExtractorType.Grib(),
    )
    builder.extend_on_axis(0)
    store = builder.build()

    data = zarr.open_array(store)

    assert data.shape[:3] == (3, 4, 3)
    assert data.chunks[:3] == (1, 1, 1)

    # Spot-checks within Part 1 (d=0,1)
    assert np.all(data[0, 0, 0] == 0)  # sfc_value(0, 0, 0)
    assert np.all(data[0, 0, 1] == 1)  # sfc_value(0, 0, 1)
    assert np.all(data[0, 1, 0] == 3)  # sfc_value(0, 1, 0)
    assert np.all(data[1, 0, 0] == 12)  # sfc_value(1, 0, 0)
    assert np.all(data[1, 3, 2] == 23)  # sfc_value(1, 3, 2)

    # Cross-part boundary: d=2 (Part 2)
    assert np.all(data[2, 0, 0] == 24)  # sfc_value(2, 0, 0) = 2*12 + 0 + 0
    assert np.all(data[2, 0, 1] == 25)  # sfc_value(2, 0, 1)
    assert np.all(data[2, 3, 2] == 35)  # sfc_value(2, 3, 2) = 24 + 9 + 2


# FixedSizeChunking: two date batches, combined date+time axis
def test_fixed_size_chunking_date_extension(
    read_only_fdb_pattern_setup,
) -> None:
    """Extension along the combined date+time axis (axis 0) using FixedSizeChunk(4).

    Both parts share the same axis structure:
      Axis 0: ["date","time"] — FixedSizeChunk(chunk_shape=4)
      Axis 1: ["param"]       — SingleValueChunking

    Part 1: dates=[2020-01-01, 2020-01-02], 4 times → combined size = 8 → 2 chunks of 4
    Part 2: date= [2020-01-03],              4 times → combined size = 4 → 1 chunk  of 4

    chunkSizeCheck for Part 2 axis ["date","time"] with cardinalities [1,4]:
      k=0: trailing=1, card=4, d=4, 4%4==0 → valid ✓

    After extend_on_axis(0): 12 combined-index values, 3 chunks of 4.

    Array layout (combined axis 0 index = d*4 + t):
      data[d*4 + t, p] = sfc_value(d, t, p) = d*12 + t*3 + p
    """
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)

    axes = [
        AxisDefinition(["date", "time"], Chunking.FixedSizeChunk(chunk_shape=4)),
        AxisDefinition(["param"], Chunking.SINGLE_VALUE),
    ]

    builder.add_part(
        {
            **COMMON,
            "levtype": "sfc",
            "date": ["2020-01-01", "2020-01-02"],
            "time": [0, 600, 1200, 1800],
            "param": [165, 166, 167],
        },
        axes,
        ExtractorType.Grib(),
    )
    builder.add_part(
        {
            **COMMON,
            "levtype": "sfc",
            "date": ["2020-01-03"],
            "time": [0, 600, 1200, 1800],
            "param": [165, 166, 167],
        },
        axes,
        ExtractorType.Grib(),
    )
    builder.extend_on_axis(0)
    store = builder.build()

    data = zarr.open_array(store)

    assert data.shape[:2] == (12, 3)
    assert data.chunks[:2] == (4, 1)

    # Combined index i = d*4 + t; value = sfc_value(d, t, p) = d*12 + t*3 + p
    # Part 1 occupies combined indices [0, 7]
    assert np.all(data[0, 0] == 0)  # (d=0,t=0), p=0 → 0
    assert np.all(data[0, 2] == 2)  # (d=0,t=0), p=2 → 2
    assert np.all(data[1, 0] == 3)  # (d=0,t=1), p=0 → 3
    assert np.all(data[3, 0] == 9)  # (d=0,t=3), p=0 → 9  (last in chunk 0)
    assert np.all(data[4, 0] == 12)  # (d=1,t=0), p=0 → 12 (first in chunk 1)
    assert np.all(data[7, 2] == 23)  # (d=1,t=3), p=2 → 12+9+2 = 23

    # Cross-part boundary: Part 2 starts at combined index 8 (d=2, t=0)
    assert np.all(data[8, 0] == 24)  # sfc_value(2, 0, 0) = 24
    assert np.all(data[8, 2] == 26)  # sfc_value(2, 0, 2) = 26
    assert np.all(data[11, 2] == 35)  # sfc_value(2, 3, 2) = 24+9+2 = 35


# FixedSizeChunking: two time halves, extend on time axis
def test_fixed_size_chunking_time_extension(
    read_only_fdb_pattern_setup,
) -> None:
    """Extension along the time axis (axis 1) using FixedSizeChunk(2).

    Both parts share all 3 dates and all 3 SFC params, but cover different
    halves of the 4-time sequence:
      Part 1: times=[0, 600]    (indices t=0,1) → 1 chunk of 2
      Part 2: times=[1200,1800] (indices t=2,3) → 1 chunk of 2

    Axes on both parts:
      Axis 0: ["date"]  — SingleValueChunking
      Axis 1: ["time"]  — FixedSizeChunk(chunk_shape=2)   ← extension axis
      Axis 2: ["param"] — SingleValueChunking

    After extend_on_axis(1): 4 time values, 2 chunks of 2.

    Array layout:
      data[d, t, p] = sfc_value(d, t, p) = d*12 + t*3 + p
    """
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)

    axes = [
        AxisDefinition(["date"], Chunking.SINGLE_VALUE),
        AxisDefinition(["time"], Chunking.FixedSizeChunk(chunk_shape=2)),
        AxisDefinition(["param"], Chunking.SINGLE_VALUE),
    ]

    builder.add_part(
        {
            **COMMON,
            "levtype": "sfc",
            "date": ["2020-01-01", "2020-01-02", "2020-01-03"],
            "time": [0, 600],
            "param": [165, 166, 167],
        },
        axes,
        ExtractorType.Grib(),
    )
    builder.add_part(
        {
            **COMMON,
            "levtype": "sfc",
            "date": ["2020-01-01", "2020-01-02", "2020-01-03"],
            "time": [1200, 1800],
            "param": [165, 166, 167],
        },
        axes,
        ExtractorType.Grib(),
    )
    builder.extend_on_axis(1)
    store = builder.build()

    data = zarr.open_array(store)

    assert data.shape[:3] == (3, 4, 3)
    assert data.chunks[:3] == (1, 2, 1)

    # Part 1 — time indices t=0,1 (times 0, 600)
    assert np.all(data[0, 0, 0] == 0)  # sfc_value(0, 0, 0)
    assert np.all(data[0, 1, 0] == 3)  # sfc_value(0, 1, 0)
    assert np.all(data[1, 0, 2] == 14)  # sfc_value(1, 0, 2) = 12+0+2

    # Cross-part boundary — time indices t=2,3 (times 1200, 1800), Part 2
    assert np.all(data[0, 2, 0] == 6)  # sfc_value(0, 2, 0)
    assert np.all(data[0, 3, 2] == 11)  # sfc_value(0, 3, 2) = 0+9+2
    assert np.all(data[2, 3, 2] == 35)  # sfc_value(2, 3, 2) = 24+9+2


# Three-part SingleValueChunking, one date per part
def test_three_part_single_value_extension(
    read_only_fdb_pattern_setup,
) -> None:
    """Three parts, one per date, SingleValueChunking on all axes.

    Demonstrates that extend_on_axis works for more than two same-structure parts.
    Each part has a single date:
      Part 1: date=[2020-01-01] → d=0
      Part 2: date=[2020-01-02] → d=1
      Part 3: date=[2020-01-03] → d=2

    After extend_on_axis(0): 3 date × 4 time × 3 param = (3, 4, 3) shape.

    Array layout:
      data[d, t, p] = sfc_value(d, t, p) = d*12 + t*3 + p
    """
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)

    axes = [
        AxisDefinition(["date"], Chunking.SINGLE_VALUE),
        AxisDefinition(["time"], Chunking.SINGLE_VALUE),
        AxisDefinition(["param"], Chunking.SINGLE_VALUE),
    ]

    for date_str in ["2020-01-01", "2020-01-02", "2020-01-03"]:
        builder.add_part(
            {
                **COMMON,
                "levtype": "sfc",
                "date": [date_str],
                "time": [0, 600, 1200, 1800],
                "param": [165, 166, 167],
            },
            axes,
            ExtractorType.Grib(),
        )
    builder.extend_on_axis(0)
    store = builder.build()

    data = zarr.open_array(store)

    assert data.shape[:3] == (3, 4, 3)
    assert data.chunks[:3] == (1, 1, 1)

    # First element of each date (d=0,1,2; t=0; p=0)
    assert np.all(data[0, 0, 0] == 0)  # sfc_value(0, 0, 0)
    assert np.all(data[1, 0, 0] == 12)  # sfc_value(1, 0, 0)
    assert np.all(data[2, 0, 0] == 24)  # sfc_value(2, 0, 0)

    # Last element of each date (t=3; p=2)
    assert np.all(data[0, 3, 2] == 11)  # sfc_value(0, 3, 2) = 0+9+2
    assert np.all(data[1, 3, 2] == 23)  # sfc_value(1, 3, 2) = 12+9+2
    assert np.all(data[2, 3, 2] == 35)  # sfc_value(2, 3, 2) = 24+9+2

    # Mid-axis spot-check
    assert np.all(data[1, 2, 1] == 19)  # sfc_value(1, 2, 1) = 12+6+1
    assert np.all(data[1, 2, 2] == 20)  # sfc_value(1, 2, 2) = 12+6+2


# FixedSizeChunking rejects a part whose axis size isn't a multiple
def test_fixed_size_chunking_misaligned_part_boundary_rejected(
    read_only_fdb_pattern_setup,
) -> None:
    """Documents the current limitation: each part's axis size must independently
    pass AxisMapper::chunkSizeCheck.

    Part 2 has a single date with FixedSizeChunk(2) on the date axis.
    chunkSizeCheck(axis=[1 date], chunkSize=2):
      trailingProduct=1, card=1, d=2, 1%2 != 0 → rejected.

    The build() call must raise RuntimeError mentioning "AxisMapper::mapAxisToChunks".
    """
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)

    def _axes_with_fsc2():
        return [
            AxisDefinition(["date"], Chunking.FixedSizeChunk(chunk_shape=2)),
            AxisDefinition(["time"], Chunking.SINGLE_VALUE),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
        ]

    builder.add_part(
        {
            **COMMON,
            "levtype": "sfc",
            "date": ["2020-01-01", "2020-01-02"],
            "time": [0, 600, 1200, 1800],
            "param": [165, 166, 167],
        },
        _axes_with_fsc2(),
        ExtractorType.Grib(),
    )
    builder.add_part(
        {
            **COMMON,
            "levtype": "sfc",
            "date": ["2020-01-03"],  # 1 date, but chunk size = 2 -> invalid
            "time": [0, 600, 1200, 1800],
            "param": [165, 166, 167],
        },
        _axes_with_fsc2(),
        ExtractorType.Grib(),
    )
    builder.extend_on_axis(0)

    with pytest.raises(RuntimeError, match="AxisMapper::mapAxisToChunks"):
        builder.build()
