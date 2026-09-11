# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""User-facing example: accessing a z3fdb store via xarray.

xarray wraps a zarr array as a labeled DataArray. Because the zarr metadata
now carries ``dimension_names``, xarray automatically assigns those names as
dimension labels — enabling ``.isel()``, ``.mean(dim=...)``, and other
label-based operations without any manual wiring.

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

xr = pytest.importorskip("xarray", reason="xarray is not installed")

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


def _open_as_dataarray(store) -> "xr.DataArray":
    """Open an FdbZarrStore as a labeled xarray DataArray.

    Opens the underlying zarr array, reads ``dimension_names`` from its
    metadata, and wraps it as a DataArray with those dimension labels.
    """
    arr = zarr.open_array(store)
    dims = list(arr.metadata.dimension_names)
    return xr.DataArray(arr, dims=dims)


def test_xarray_open_single_part(read_only_fdb_pattern_setup) -> None:
    """A single-part SFC view opened as a labeled xarray DataArray.

    Verifies that:
    - The DataArray dimensions match the AxisDefinition names + "values".
    - Individual elements are accessible via ``.isel()`` with named dims.
    - Values match the known formula sfc_value(d, t, p) = d*12 + t*3 + p.

    Array layout:
      data[date, time, param] = sfc_value(d, t, p)
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
            AxisDefinition(["date"], Chunking.SINGLE_VALUE, name="date"),
            AxisDefinition(["time"], Chunking.SINGLE_VALUE, name="time"),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE, name="param"),
        ],
        ExtractorType.Grib(),
    )
    store = builder.build()

    da = _open_as_dataarray(store)

    assert da.dims[:3] == ("date", "time", "param")
    assert da.sizes["date"] == 3
    assert da.sizes["time"] == 4
    assert da.sizes["param"] == 3

    # isel by named dimension — no axis indices needed
    assert np.all(da.isel(date=0, time=0, param=0).values == 0)  # sfc_value(0,0,0)
    assert np.all(da.isel(date=2, time=3, param=2).values == 35)  # sfc_value(2,3,2)


def test_xarray_open_fixed_size_chunking(read_only_fdb_pattern_setup) -> None:
    """FixedSizeChunking on the combined date+time axis opened as a DataArray.

    FixedSizeChunk(4) groups 4 consecutive date×time combinations into one
    zarr chunk. The xarray DataArray sees a flat axis of size 12.

    Array layout (combined index i = d*4 + t):
      data[date_time, param] = sfc_value(d, t, p) = d*12 + t*3 + p
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
            AxisDefinition(["param"], Chunking.SINGLE_VALUE, name="param"),
        ],
        ExtractorType.Grib(),
    )
    store = builder.build()

    da = _open_as_dataarray(store)

    assert da.dims[:2] == ("date_time", "param")
    assert da.sizes["date_time"] == 12
    assert da.sizes["param"] == 3

    assert np.all(da.isel(date_time=0, param=0).values == 0)  # sfc_value(0,0,0)
    assert np.all(da.isel(date_time=11, param=2).values == 35)  # sfc_value(2,3,2)
    assert np.all(da.isel(date_time=4, param=0).values == 12)  # sfc_value(1,0,0)


def test_xarray_multi_part_sfc_pl(read_only_fdb_pattern_setup) -> None:
    """Multi-part SFC + PL view opened as a DataArray.

    Both parts share the same date×time axis (12 values). They extend along
    axis 1: SFC params occupy indices 0–2, PL param×level indices 3–11.

    Array layout:
      Axis 0 "date_time": 12 combined date×time values
      Axis 1 "variable":  3 SFC + 9 PL = 12 combined values
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
            AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE, name="date_time"),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE, name="variable"),
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
            AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE, name="date_time"),
            AxisDefinition(["param", "levelist"], Chunking.SINGLE_VALUE, name="variable"),
        ],
        ExtractorType.Grib(),
    )
    builder.extend_on_axis(1)
    store = builder.build()

    da = _open_as_dataarray(store)

    assert da.dims[:2] == ("date_time", "variable")
    assert da.sizes["date_time"] == 12
    assert da.sizes["variable"] == 12

    # SFC: (d=0,t=0) → date_time=0; param=165 → variable=0; sfc_value(0,0,0) = 0
    assert np.all(da.isel(date_time=0, variable=0).values == 0)
    # PL: (d=0,t=0) → date_time=0; param=131,level=50 → variable=3; pl_value(0,0,0,0) = 36
    assert np.all(da.isel(date_time=0, variable=3).values == 36)
    # PL corner: date_time=11; variable=11; pl_value(2,3,2,2) = 36+72+27+6+2 = 143
    assert np.all(da.isel(date_time=11, variable=11).values == 143)


def test_xarray_reduction_over_named_dim(read_only_fdb_pattern_setup) -> None:
    """xarray reduction along a named dimension.

    Computes the mean over the time axis using the dimension label rather than
    an integer axis index. The result is compared against the reference formula.

    sfc_value(d, t, p) = d*12 + t*3 + p
    mean over t in [0,1,2,3]: d*12 + (0+3+6+9)/4 + p = d*12 + 4.5 + p
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
            AxisDefinition(["date"], Chunking.SINGLE_VALUE, name="date"),
            AxisDefinition(["time"], Chunking.SINGLE_VALUE, name="time"),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE, name="param"),
        ],
        ExtractorType.Grib(),
    )
    store = builder.build()

    da = _open_as_dataarray(store)

    # Mean over the "time" dimension by name — no axis integer needed
    time_mean = da.mean(dim="time")

    assert time_mean.dims[:2] == ("date", "param")

    for d in range(3):
        for p in range(3):
            expected = d * 12 + 4.5 + p
            assert np.allclose(time_mean.isel(date=d, param=p).values, expected), (
                f"time mean mismatch at d={d}, p={p}: "
                f"got {float(time_mean.isel(date=d, param=p).values[0]):.2f}, "
                f"expected {expected:.2f}"
            )
