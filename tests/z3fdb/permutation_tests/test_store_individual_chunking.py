# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

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
