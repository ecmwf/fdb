# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""User-facing example: accessing a z3fdb store via dask.

Dask treats a Zarr array as a chunked graph — each Zarr chunk becomes one
dask task. Because z3fdb exposes a fully conformant Zarr v3 store, dask.array
can open it directly and schedule FDB retrievals lazily.

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

da = pytest.importorskip("dask.array", reason="dask is not installed")

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
    "step": 0,
}


def test_dask_open_single_part(read_only_fdb_pattern_setup) -> None:
    """A single-part SFC view opened as a dask array.

    Verifies that:
    - dask.array.from_zarr accepts an FdbZarrStore without materialising data.
    - The dask graph chunk shape matches the z3fdb chunk configuration.
    - Computing individual chunks triggers FDB retrieval and returns correct values.

    Array layout:
      data[d, t, p] = sfc_value(d, t, p) = d*12 + t*3 + p
    """
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        {
            **COMMON,
            "levtype": "sfc",
            "date": ["2020-01-01", "2020-01-02", "2020-01-03"],
            "time": [0, 600, 1200, 1800],
            "param": [165, 166, 167],
        },
        [
            AxisDefinition(["date"], Chunking.SINGLE_VALUE),
            AxisDefinition(["time"], Chunking.SINGLE_VALUE),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.Grib(),
    )
    store = builder.build()

    # Open via zarr first, then wrap with dask — the standard pattern
    zarr_array = zarr.open_array(store)
    dask_array = da.from_zarr(zarr_array)

    # Lazy: no FDB retrieval yet
    assert isinstance(dask_array, da.Array)
    assert dask_array.shape[:3] == (3, 4, 3)
    assert dask_array.chunks[:3] == ((1, 1, 1), (1, 1, 1, 1), (1, 1, 1))

    # Compute a single scalar (triggers one FDB retrieve for that chunk)
    val = dask_array[0, 0, 0].compute()
    assert np.all(val == 0)  # sfc_value(0, 0, 0)

    val = dask_array[2, 3, 2].compute()
    assert np.all(val == 35)  # sfc_value(2, 3, 2) = 24+9+2


def test_dask_open_fixed_size_chunking(read_only_fdb_pattern_setup) -> None:
    """FixedSizeChunking on the combined date+time axis opened as a dask array.

    FixedSizeChunk(4) on the ["date","time"] axis groups 4 consecutive
    date×time combinations into one Zarr chunk → one dask task.

    Array layout (combined axis 0 index i = d*4 + t):
      data[i, p] = sfc_value(d, t, p) = d*12 + t*3 + p
    """
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        {
            **COMMON,
            "levtype": "sfc",
            "date": ["2020-01-01", "2020-01-02", "2020-01-03"],
            "time": [0, 600, 1200, 1800],
            "param": [165, 166, 167],
        },
        [
            AxisDefinition(["date", "time"], Chunking.FixedSizeChunk(chunk_shape=4)),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.Grib(),
    )
    store = builder.build()

    zarr_array = zarr.open_array(store)
    dask_array = da.from_zarr(zarr_array)

    # 12 date×time values in 3 chunks of 4; 3 param values each in 1 chunk
    assert dask_array.shape[:2] == (12, 3)
    assert dask_array.chunks[:2] == ((4, 4, 4), (1, 1, 1))

    # Spot-check: first element of first chunk
    assert np.all(dask_array[0, 0].compute() == 0)  # sfc_value(0, 0, 0)
    # Spot-check: last element of last chunk (chunk boundary)
    assert np.all(dask_array[11, 2].compute() == 35)  # sfc_value(2, 3, 2)
    # Chunk boundary: index 4 is the first element of the second chunk
    assert np.all(dask_array[4, 0].compute() == 12)  # sfc_value(1, 0, 0)


def test_dask_multi_part_sfc_pl(read_only_fdb_pattern_setup) -> None:
    """Multi-part SFC + PL view with two-axis dask graph.

    Both parts use SingleValueChunking. Part 1 (SFC) occupies param indices
    0–2; Part 2 (PL) occupies param indices 3–11 on the extension axis.

    Array layout:
      Axis 0: combined date×time  (12 values, SINGLE_VALUE → 12 chunks of 1)
      Axis 1: SFC param / PL param×levelist  (3 + 9 = 12 values, SINGLE_VALUE)
    """
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        {
            **COMMON,
            "levtype": "sfc",
            "date": ["2020-01-01", "2020-01-02", "2020-01-03"],
            "time": [0, 600, 1200, 1800],
            "param": [165, 166, 167],
        },
        [
            AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.Grib(),
    )
    builder.add_part(
        {
            **COMMON,
            "levtype": "pl",
            "date": ["2020-01-01", "2020-01-02", "2020-01-03"],
            "time": [0, 600, 1200, 1800],
            "param": [131, 132, 133],
            "levelist": [50, 100, 150],
        },
        [
            AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
            AxisDefinition(["param", "levelist"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.Grib(),
    )
    builder.extend_on_axis(1)
    store = builder.build()

    zarr_array = zarr.open_array(store)
    dask_array = da.from_zarr(zarr_array)

    assert dask_array.shape[:2] == (12, 12)

    # SFC spot-check: (d=0,t=0) → combined index 0; param=165 → axis-1 index 0
    # sfc_value(0, 0, 0) = 0
    assert np.all(dask_array[0, 0].compute() == 0)

    # PL spot-check: (d=0,t=0) → combined index 0; param=131,levelist=50 → axis-1 index 3
    # pl_value(0, 0, 0, 0) = 36
    assert np.all(dask_array[0, 3].compute() == 36)

    # PL corner: (d=2,t=3) → combined index 11; param=133,levelist=150 → axis-1 index 11
    # pl_value(2, 3, 2, 2) = 36 + 72 + 27 + 6 + 2 = 143
    assert np.all(dask_array[11, 11].compute() == 143)


def test_dask_reduction(read_only_fdb_pattern_setup) -> None:
    """Dask reduction across the full SFC array.

    Computes the mean over the time axis using dask's lazy graph. The result
    is compared against the numpy reference computed from the known value formula.

    sfc_value(d, t, p) = d*12 + t*3 + p
    mean over t in [0,1,2,3]: (d*12 + 0 + d*12 + 3 + d*12 + 6 + d*12 + 9 + 4*p) / 4
                             = d*12 + 4.5 + p
    """
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        {
            **COMMON,
            "levtype": "sfc",
            "date": ["2020-01-01", "2020-01-02", "2020-01-03"],
            "time": [0, 600, 1200, 1800],
            "param": [165, 166, 167],
        },
        [
            AxisDefinition(["date"], Chunking.SINGLE_VALUE),
            AxisDefinition(["time"], Chunking.SINGLE_VALUE),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.Grib(),
    )
    store = builder.build()

    zarr_array = zarr.open_array(store)
    dask_array = da.from_zarr(zarr_array)

    # Mean over time axis (axis 1), result shape: (3 dates, 3 params, N grid points)
    time_mean = dask_array.mean(axis=1).compute()

    # Expected: mean_value(d, p) = d*12 + 4.5 + p
    for d in range(3):
        for p in range(3):
            expected = d * 12 + 4.5 + p
            assert np.allclose(time_mean[d, p], expected), (
                f"time mean mismatch at d={d}, p={p}: got {time_mean[d, p, 0]:.2f}, expected {expected:.2f}"
            )
