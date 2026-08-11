# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import pytest
import numpy as np
import zarr

from z3fdb import (
    AxisDefinition,
    Chunking,
    ExtractorType,
    SimpleStoreBuilder,
)

pytestmark = pytest.mark.offline

# Both requests span the full pattern-fixture domain:
#   dates      = [20200101, 20200102, 20200103]  (3 values, 0-based index d)
#   times      = [0, 600, 1200, 1800]            (4 values, 0-based index t)
#   params_sfc = [165, 166, 167]                 (3 values, 0-based index p)
#   params_pl  = [131, 132, 133]                 (3 values, 0-based index p)
#   levels     = [50, 100, 150]                  (3 values, 0-based index l)
#
# Field-value formulas (derived from conftest.py build_pattern_grib_messages):
#   sfc_value(d, t, p)    = d*12 + t*3 + p
#   pl_value(d, t, p, l)  = 36 + d*36 + t*9 + p*3 + l

SFC_REQUEST = {
    "type": "an", "class": "ea", "domain": "g", "expver": "0001", "stream": "oper",
    "date": ["2020-01-01", "2020-01-02", "2020-01-03"],
    "levtype": "sfc", "step": 0,
    "param": [165, 166, 167],
    "time": [0, 600, 1200, 1800],
}

PL_REQUEST = {
    "type": "an", "class": "ea", "domain": "g", "expver": "0001", "stream": "oper",
    "date": ["2020-01-01", "2020-01-02", "2020-01-03"],
    "levtype": "pl", "step": 0,
    "param": [131, 132, 133],
    "levelist": [50, 100, 150],
    "time": [0, 600, 1200, 1800],
}


def test_individual_chunking_combined_datetime_axis(
    read_only_fdb_pattern_setup,
) -> None:
    """Two-part view (SFC + PL) with FixedSizeChunk on both axes; the extension
    axis definitions differ in key-dimensionality between parts.

    Both parts share a 2-key combined axis 0 (["date","time"], FSC{4}):
      12 values, 3 chunks of 4.

    Axis 1 is the extension axis with FSC{3} in both parts but different
    key-dimensionality:
      Part 1 (SFC): ["param"]            -- 1-key axis, 3 values, 1 chunk of 3
      Part 2 (PL):  ["param","levelist"] -- 2-key combined axis, 9 values, 3 chunks of 3

    After extend_on_axis(1): axis-1 = 3+9 = 12 values, 4 chunks of 3.

    Array layout:
      Axis 0: combined date+time (12 values, FSC{4}, chunk size 4)
              index = date_idx * 4 + time_idx
      Axis 1: SFC param (indices 0-2) / PL param+levelist (indices 3-11)
              PL index = 3 + param_idx * 3 + level_idx  (param outer, levelist inner)
      Axis 2: implicit field values (N grid points)
    """
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        SFC_REQUEST,
        [
            AxisDefinition(["date", "time"], Chunking.FixedSizeChunk(chunkShape=4)),
            AxisDefinition(["param"], Chunking.FixedSizeChunk(chunkShape=3)),
        ],
        ExtractorType.GRIB,
    )
    builder.add_part(
        PL_REQUEST,
        [
            AxisDefinition(["date", "time"], Chunking.FixedSizeChunk(chunkShape=4)),
            AxisDefinition(["param", "levelist"], Chunking.FixedSizeChunk(chunkShape=3)),
        ],
        ExtractorType.GRIB,
    )
    builder.extend_on_axis(1)
    store = builder.build()

    data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)

    # Shape: combined date*time = 12; SFC param (3) + PL param*levelist (9) = 12
    assert data.shape[:2] == (12, 12)
    # Chunk shape: FSC{4} on combined datetime axis (12/4=3 chunks); FSC{3} on param axis
    assert data.chunks[:2] == (4, 3)

    # SFC spot checks -- sfc_value(d, t, p) = d*12 + t*3 + p
    assert np.all(data[0, 0] == 0)  # (20200101, 0),   param=165
    assert np.all(data[0, 1] == 1)  # (20200101, 0),   param=166
    assert np.all(data[0, 2] == 2)  # (20200101, 0),   param=167
    assert np.all(data[1, 0] == 3)  # (20200101, 600), param=165
    assert np.all(data[4, 0] == 12)  # (20200102, 0),   param=165

    # PL spot checks -- pl_value(d, t, p, l) = 36 + d*36 + t*9 + p*3 + l
    # PL axis-1 indices start at 3; param outer (slowest), levelist inner (fastest)
    assert np.all(data[0, 3] == 36)  # (20200101, 0),   param=131, levelist=50
    assert np.all(data[0, 4] == 37)  # (20200101, 0),   param=131, levelist=100
    assert np.all(data[0, 5] == 38)  # (20200101, 0),   param=131, levelist=150
    assert np.all(data[0, 6] == 39)  # (20200101, 0),   param=132, levelist=50
    # pl_value(1, 0, 0, 0) = 36 + 36 + 0 + 0 + 0 = 72
    assert np.all(data[4, 3] == 72)  # (20200102, 0),   param=131, levelist=50

    # Corner cases -- max indices and chunk boundaries
    # Last SFC: combined idx 11=(d=2,t=3=1800), param idx 2=167
    # sfc_value(2, 3, 2) = 2*12 + 3*3 + 2 = 35
    assert np.all(data[11, 2] == 35)  # (20200103, 1800), param=167
    # Last PL: combined idx 11=(d=2,t=3=1800), PL idx 8=(p=133,l=150)
    # pl_value(2, 3, 2, 2) = 36 + 72 + 27 + 6 + 2 = 143
    assert np.all(data[11, 11] == 143)  # (20200103, 1800), param=133, levelist=150
    # FSC{4} axis-0 chunk boundary: last entry in chunk 0 (combined idx 3 = d=0,t=3=1800)
    # sfc_value(0, 3, 0) = 0 + 9 + 0 = 9
    assert np.all(data[3, 0] == 9)  # (20200101, 1800), param=165
    # SFC/PL axis-1 boundary: first PL entry at max combined index
    # pl_value(2, 3, 0, 0) = 36 + 72 + 27 + 0 + 0 = 135
    assert np.all(data[11, 3] == 135)  # (20200103, 1800), param=131, levelist=50


def test_individual_chunking_separate_time_axis(
    read_only_fdb_pattern_setup,
) -> None:
    """Two-part view (SFC + PL) with FixedSizeChunk on the time axis and the
    extension axis; the extension axis definitions differ in key-dimensionality.

    Both parts share axis-0 (["date"], SINGLE_VALUE) and axis-1
    (["time"], FSC{2}), giving 2 chunks of 2 on the time dimension.

    Axis 2 is the extension axis with FSC{3} in both parts but different
    key-dimensionality:
      Part 1 (SFC): ["param"]            -- 1-key axis, 3 values, 1 chunk of 3
      Part 2 (PL):  ["param","levelist"] -- 2-key combined axis, 9 values, 3 chunks of 3

    After extend_on_axis(2): axis-2 = 3+9 = 12 values, 4 chunks of 3.

    Array layout:
      Axis 0: date (3 values, SINGLE_VALUE, chunk size 1)
      Axis 1: time (4 values, FSC{2}, chunk size 2)
      Axis 2: SFC param (indices 0-2) / PL param+levelist (indices 3-11)
              PL index = 3 + param_idx * 3 + level_idx  (param outer, levelist inner)
      Axis 3: implicit field values (N grid points)
    """
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        SFC_REQUEST,
        [
            AxisDefinition(["date"], Chunking.SINGLE_VALUE),
            AxisDefinition(["time"], Chunking.FixedSizeChunk(chunkShape=2)),
            AxisDefinition(["param"], Chunking.FixedSizeChunk(chunkShape=3)),
        ],
        ExtractorType.GRIB,
    )
    builder.add_part(
        PL_REQUEST,
        [
            AxisDefinition(["date"], Chunking.SINGLE_VALUE),
            AxisDefinition(["time"], Chunking.FixedSizeChunk(chunkShape=2)),
            AxisDefinition(["param", "levelist"], Chunking.FixedSizeChunk(chunkShape=3)),
        ],
        ExtractorType.GRIB,
    )
    builder.extend_on_axis(2)
    store = builder.build()

    data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)

    # Shape: date=3, time=4; SFC param (3) + PL param*levelist (9) = 12
    assert data.shape[:3] == (3, 4, 12)
    # Chunk shape: SINGLE_VALUE (1) on date; FSC{2} on time; FSC{3} on param axis
    assert data.chunks[:3] == (1, 2, 3)

    # SFC spot checks -- sfc_value(d, t, p) = d*12 + t*3 + p
    assert np.all(data[0, 0, 0] == 0)  # date=20200101, time=0,   param=165
    assert np.all(data[0, 0, 1] == 1)  # date=20200101, time=0,   param=166
    assert np.all(data[0, 0, 2] == 2)  # date=20200101, time=0,   param=167
    assert np.all(data[0, 1, 0] == 3)  # date=20200101, time=600, param=165
    assert np.all(data[1, 0, 0] == 12)  # date=20200102, time=0,   param=165

    # PL spot checks -- pl_value(d, t, p, l) = 36 + d*36 + t*9 + p*3 + l
    # PL axis-2 indices start at 3; param outer (slowest), levelist inner (fastest)
    assert np.all(data[0, 0, 3] == 36)  # date=20200101, time=0,   param=131, levelist=50
    assert np.all(data[0, 0, 4] == 37)  # date=20200101, time=0,   param=131, levelist=100
    assert np.all(data[0, 0, 5] == 38)  # date=20200101, time=0,   param=131, levelist=150
    assert np.all(data[0, 0, 6] == 39)  # date=20200101, time=0,   param=132, levelist=50
    # pl_value(1, 1, 0, 0) = 36 + 36 + 9 + 0 + 0 = 81
    assert np.all(data[1, 1, 3] == 81)  # date=20200102, time=600, param=131, levelist=50

    # Corner cases -- max indices and chunk boundaries
    # Last SFC: date idx 2=20200103, time idx 3=1800, param idx 2=167
    # sfc_value(2, 3, 2) = 2*12 + 3*3 + 2 = 35
    assert np.all(data[2, 3, 2] == 35)  # date=20200103, time=1800, param=167
    # Last PL: date idx 2, time idx 3=1800, PL idx 8=(p=133,l=150)
    # pl_value(2, 3, 2, 2) = 36 + 72 + 27 + 6 + 2 = 143
    assert np.all(data[2, 3, 11] == 143)  # date=20200103, time=1800, param=133, levelist=150
    # FSC{2} time chunk boundary: last time entry (idx 3=1800) on SFC side
    # sfc_value(0, 3, 0) = 0 + 9 + 0 = 9
    assert np.all(data[0, 3, 0] == 9)  # date=20200101, time=1800, param=165
    # SFC/PL axis-2 boundary: first PL entry at max date+time
    # pl_value(2, 3, 0, 0) = 36 + 72 + 27 + 0 + 0 = 135
    assert np.all(data[2, 3, 3] == 135)  # date=20200103, time=1800, param=131, levelist=50


def test_individual_chunking_reordered_axes(
    read_only_fdb_pattern_setup,
) -> None:
    """Two-part view (SFC + PL) where the axis order in the view definition is
    reversed relative to the natural request key order.

    The MARS requests enumerate keys in the order date/time/param(/levelist),
    but the AxisDefinition list places them in a different order: param (axis 0,
    extension), time (axis 1), date (axis 2).

    The extension axis (axis 0) has FSC{3} in both parts but different
    key-dimensionality:
      Part 1 (SFC): ["param"]            -- 1-key axis, 3 values, 1 chunk of 3
      Part 2 (PL):  ["param","levelist"] -- 2-key combined axis, 9 values, 3 chunks of 3

    After extend_on_axis(0): axis-0 = 3+9 = 12 values, 4 chunks of 3.

    Array layout:
      Axis 0: SFC param (indices 0-2) / PL param+levelist (indices 3-11)
              PL index = 3 + param_idx * 3 + level_idx  (param outer, levelist inner)
      Axis 1: time (4 values, FSC{2}, chunk size 2)
      Axis 2: date (3 values, SINGLE_VALUE, chunk size 1)
      Axis 3: implicit field values (N grid points)
    """
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        SFC_REQUEST,
        [
            AxisDefinition(["param"], Chunking.FixedSizeChunk(chunkShape=3)),
            AxisDefinition(["time"], Chunking.FixedSizeChunk(chunkShape=2)),
            AxisDefinition(["date"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )
    builder.add_part(
        PL_REQUEST,
        [
            AxisDefinition(["param", "levelist"], Chunking.FixedSizeChunk(chunkShape=3)),
            AxisDefinition(["time"], Chunking.FixedSizeChunk(chunkShape=2)),
            AxisDefinition(["date"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )
    builder.extend_on_axis(0)
    store = builder.build()

    data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)

    # Shape: SFC param (3) + PL param*levelist (9) = 12; time=4; date=3
    assert data.shape[:3] == (12, 4, 3)
    # Chunk shape: FSC{3} on param axis; FSC{2} on time; SINGLE_VALUE (1) on date
    assert data.chunks[:3] == (3, 2, 1)

    # SFC spot checks -- sfc_value(d, t, p) = d*12 + t*3 + p
    # axis-0=p_idx, axis-1=t_idx, axis-2=d_idx
    assert np.all(data[0, 0, 0] == 0)  # param=165, time=0,   date=20200101
    assert np.all(data[1, 0, 0] == 1)  # param=166, time=0,   date=20200101
    assert np.all(data[2, 0, 0] == 2)  # param=167, time=0,   date=20200101
    assert np.all(data[0, 1, 0] == 3)  # param=165, time=600, date=20200101
    assert np.all(data[0, 0, 1] == 12)  # param=165, time=0,   date=20200102

    # PL spot checks -- pl_value(d, t, p, l) = 36 + d*36 + t*9 + p*3 + l
    # PL axis-0 indices start at 3; param outer (slowest), levelist inner (fastest)
    assert np.all(data[3, 0, 0] == 36)  # param=131, levelist=50,  time=0,   date=20200101
    assert np.all(data[4, 0, 0] == 37)  # param=131, levelist=100, time=0,   date=20200101
    assert np.all(data[5, 0, 0] == 38)  # param=131, levelist=150, time=0,   date=20200101
    assert np.all(data[6, 0, 0] == 39)  # param=132, levelist=50,  time=0,   date=20200101
    # pl_value(1, 1, 0, 0) = 36 + 36 + 9 + 0 + 0 = 81
    assert np.all(data[3, 1, 1] == 81)  # param=131, levelist=50,  time=600, date=20200102

    # Corner cases -- max indices and chunk boundaries
    # Last SFC: param idx 2=167, time idx 3=1800, date idx 2=20200103
    # sfc_value(2, 3, 2) = 2*12 + 3*3 + 2 = 35
    assert np.all(data[2, 3, 2] == 35)  # param=167, time=1800, date=20200103
    # Last PL: PL idx 8=(p=133,l=150), time idx 3=1800, date idx 2=20200103
    # pl_value(2, 3, 2, 2) = 36 + 72 + 27 + 6 + 2 = 143
    assert np.all(data[11, 3, 2] == 143)  # param=133, levelist=150, time=1800, date=20200103
    # FSC{2} time chunk boundary: last time entry (idx 3=1800)
    # sfc_value(0, 3, 0) = 0 + 9 + 0 = 9
    assert np.all(data[0, 3, 0] == 9)  # param=165, time=1800, date=20200101
    # SFC/PL axis-0 boundary: first PL entry at max time+date
    # pl_value(2, 3, 0, 0) = 36 + 72 + 27 + 0 + 0 = 135
    assert np.all(data[3, 3, 2] == 135)  # param=131, levelist=50, time=1800, date=20200103


# Valid FixedSizeChunk sizes for the mixed-levtype single-axis test.
#
# A chunk size C is valid when C = trailingProduct x d, where trailingProduct is the
# product of cardinalities of the k fastest-varying keys and d divides the (k+1)-th key's
# cardinality.  Every chunk must cover the inner (faster) keys in full.
#
# Part 1 - PL:  date(1) x time(4) x param(3) x level(3), axis ["date","time","param","levelist"]
#   k=0 trailing=1,  split at level(3):  d | 3  -> C in {1, 3}
#   k=1 trailing=3,  split at param(3):  d | 3  -> C in {3, 9}
#   k=2 trailing=9,  split at time(4):   d | 4  -> C in {9, 18, 36}
#   k=3 trailing=36, split at date(1):   d | 1  -> C in {36}
#   -> valid: {1, 3, 9, 18, 36}
#
# Part 2 - SFC: date(2) x time(3) x param(3), axis ["date","time","param"]
#   k=0 trailing=1, split at param(3):  d | 3  -> C in {1, 3}
#   k=1 trailing=3, split at time(3):   d | 3  -> C in {3, 9}
#   k=2 trailing=9, split at date(2):   d | 2  -> C in {9, 18}
#   -> valid: {1, 3, 9, 18}
#
# Cross-part intersection (chunkingConsistencyCheck requirement): {1, 3, 9, 18}
_FOUR_KEY_VALID_CHUNK_SIZES = [1, 3, 9, 18]


@pytest.mark.parametrize("chunk_size", _FOUR_KEY_VALID_CHUNK_SIZES)
def test_individual_chunking_four_key_single_axis(
    read_only_fdb_pattern_setup, chunk_size: int
) -> None:
    """Mixed-levtype view: Part 1 is PL (four-key axis), Part 2 is SFC (three-key axis).

      Part 1 (PL):  date=[2020-01-01],                             1 x 4 x 3 x 3 = 36 values
                    axis ["date", "time", "param", "levelist"]
      Part 2 (SFC): date=[2020-01-01, 2020-01-02], time=[0,600,1200], 2 x 3 x 3 = 18 values
                    axis ["date", "time", "param"]

    After extend_on_axis(0): combined axis = 36 + 18 = 54 values.

    Valid chunk sizes (see _FOUR_KEY_VALID_CHUNK_SIZES above) are the cross-part
    intersection of chunk sizes accepted by AxisMapper::chunkSizeCheck for each part.

    Field-value formulas (from conftest.py, all indices 0-based):
      pl_value(d, t, p, l) = 36 + d*36 + t*9 + p*3 + l
      sfc_value(d, t, p)   = d*12 + t*3 + p

    Axis index formulas:
      Part 1 (i in [0, 36)):  i = t*9 + p*3 + l         -> data[i] = 36 + i
      Part 2 (i in [36, 54)): i = 36 + d*9 + t*3 + p    -> data[i] = d*12 + t*3 + p
        (date stride in index is 9 = 3 times x 3 params;
         fixture value uses stride 12 = 4 times x 3 params)
    """
    NUM_TIMES = 4  # PL Part 1
    NUM_PARAMS = 3
    NUM_LEVELS = 3  # PL only

    PART1_SIZE = 1 * NUM_TIMES * NUM_PARAMS * NUM_LEVELS  # = 36
    PART2_SIZE = 2 * 3 * NUM_PARAMS  # date(2)*time(3)*param(3) = 18

    assert PART1_SIZE % chunk_size == 0
    assert PART2_SIZE % chunk_size == 0

    COMMON = {
        "type": "an", "class": "ea", "domain": "g", "expver": "0001",
        "stream": "oper", "step": 0,
    }

    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        {
            **COMMON,
            "levtype": "pl",
            "date": ["2020-01-01"],
            "time": [0, 600, 1200, 1800],
            "param": [131, 132, 133],
            "levelist": [50, 100, 150],
        },
        [
            AxisDefinition(
                ["date", "time", "param", "levelist"],
                Chunking.FixedSizeChunk(chunkShape=chunk_size),
            )
        ],
        ExtractorType.GRIB,
    )
    builder.add_part(
        {
            **COMMON,
            "levtype": "sfc",
            "date": ["2020-01-01", "2020-01-02"],
            "time": [0, 600, 1200],
            "param": [165, 166, 167],
        },
        [
            AxisDefinition(
                ["date", "time", "param"],
                Chunking.FixedSizeChunk(chunkShape=chunk_size),
            )
        ],
        ExtractorType.GRIB,
    )
    builder.extend_on_axis(0)
    store = builder.build()

    data = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)

    assert data.shape[0] == PART1_SIZE + PART2_SIZE  # = 54
    assert data.chunks[0] == chunk_size

    # Part 1 (i in [0, 36)): i = t*9 + p*3 + l, data[i] = pl_value(0,t,p,l) = 36 + i
    assert np.all(data[0] == 36)   # t=0,    p=131, l=50  -> pl_value(0,0,0,0) = 36
    assert np.all(data[1] == 37)   # t=0,    p=131, l=100
    assert np.all(data[3] == 39)   # t=0,    p=132, l=50
    assert np.all(data[9] == 45)   # t=600,  p=131, l=50  -> pl_value(0,1,0,0) = 45
    assert np.all(data[35] == 71)  # t=1800, p=133, l=150 -> pl_value(0,3,2,2) = 71

    # Part 1 -> Part 2 boundary at index 36 (chunk-aligned: PART1_SIZE % chunk_size == 0)
    assert PART1_SIZE % chunk_size == 0
    # Part 2 (i in [36, 54)): i = 36 + d*9 + t*3 + p, data[i] = sfc_value(d,t,p) = d*12 + t*3 + p
    assert np.all(data[36] == 0)   # d=0, t=0,    p=165 -> sfc_value(0,0,0) = 0
    assert np.all(data[37] == 1)   # d=0, t=0,    p=166
    assert np.all(data[44] == 8)   # d=0, t=1200, p=167 -> sfc_value(0,2,2) = 8
    assert np.all(data[45] == 12)  # d=1, t=0,    p=165 -> sfc_value(1,0,0) = 12
    assert np.all(data[53] == 20)  # d=1, t=1200, p=167 -> sfc_value(1,2,2) = 20


# Chunk sizes that violate the trailing-product rule for the four-key PL axis.
# The axis ["date","time","param","levelist"] with cardinalities [1,4,3,3]
# has valid sizes {1, 3, 9, 18, 36}.  Every other positive value must be rejected.
_FOUR_KEY_INVALID_CHUNK_SIZES = [2, 4, 5, 6, 7, 8, 10, 12]


@pytest.mark.parametrize("invalid_chunk_size", _FOUR_KEY_INVALID_CHUNK_SIZES)
def test_individual_chunking_rejects_invalid_chunk_size(
    read_only_fdb_pattern_setup, invalid_chunk_size: int
) -> None:
    """build() raises when the chunk size violates the key-hierarchy alignment rule.

    Uses the PL axis ["date","time","param","levelist"] with cardinalities [1,4,3,3].
    AxisMapper::chunkSizeCheck returns false for any size not in {1, 3, 9, 18, 36},
    mapAxisToChunks raises AxisMapperException, and ChunkedDataViewBuilder::build()
    re-raises it as a RuntimeError whose message contains "AxisMapper::mapAxisToChunks".
    """
    COMMON = {
        "type": "an", "class": "ea", "domain": "g", "expver": "0001",
        "stream": "oper", "step": 0,
    }
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        {
            **COMMON,
            "levtype": "pl",
            "date": ["2020-01-01"],
            "time": [0, 600, 1200, 1800],
            "param": [131, 132, 133],
            "levelist": [50, 100, 150],
        },
        [
            AxisDefinition(
                ["date", "time", "param", "levelist"],
                Chunking.FixedSizeChunk(chunkShape=invalid_chunk_size),
            )
        ],
        ExtractorType.GRIB,
    )
    with pytest.raises(RuntimeError, match="AxisMapper::mapAxisToChunks"):
        builder.build()
