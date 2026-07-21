"""Tests for opening a z3fdb zarr store with xarray.

These tests verify that xarray.open_zarr can open FdbZarrStore objects and
that dimension_names from AxisDefinition are exposed as xarray dimension names.

Requires xarray to be installed; tests are skipped automatically otherwise.
"""

import numpy as np
import pytest

xr = pytest.importorskip("xarray", reason="xarray not installed")

from z3fdb import AxisDefinition, Chunking, ExtractorType
from z3fdb.custom_store_builder import CustomStoreBuilder


def test_open_zarr_group_with_xarray_root_arrays(read_only_fdb_pattern_setup) -> None:
    """xarray.open_zarr must expose arrays at the root level as Dataset variables.

    Dimension names from AxisDefinition are reflected as xarray dimension names.
    """
    builder = CustomStoreBuilder(read_only_fdb_pattern_setup)
    for param, name in [(165, "wind_u"), (166, "wind_v")]:
        builder.add_view(
            [name],
            f"type=an,class=ea,domain=g,expver=0001,stream=oper,"
            f"date=2020-01-01/2020-01-02/2020-01-03,levtype=sfc,step=0,"
            f"param={param},time=0/600/1200/1800",
            [
                AxisDefinition(
                    ["date", "time"], Chunking.SINGLE_VALUE, dim_name="valid_time"
                )
            ],
            ExtractorType.GRIB,
        )
    store = builder.build()

    ds = xr.open_zarr(store, consolidated=False, zarr_format=3)

    assert set(ds.data_vars) == {"wind_u", "wind_v"}
    assert ds["wind_u"].dims[0] == "valid_time"
    assert ds["wind_v"].dims[0] == "valid_time"
    # 3 dates × 4 times = 12 steps
    assert ds["wind_u"].shape[0] == 12
    assert ds["wind_v"].shape[0] == 12
    assert ds["wind_u"].dtype == "float32"


def test_open_zarr_group_with_xarray_nested_group(read_only_fdb_pattern_setup) -> None:
    """xarray.open_zarr with group= must expose arrays in a named subgroup."""
    builder = CustomStoreBuilder(read_only_fdb_pattern_setup)
    for param, name in [(165, "wind_u"), (166, "wind_v")]:
        builder.add_view(
            ["sfc", name],
            f"type=an,class=ea,domain=g,expver=0001,stream=oper,"
            f"date=2020-01-01/2020-01-02/2020-01-03,levtype=sfc,step=0,"
            f"param={param},time=0/600/1200/1800",
            [
                AxisDefinition(
                    ["date", "time"], Chunking.SINGLE_VALUE, dim_name="valid_time"
                )
            ],
            ExtractorType.GRIB,
        )
    store = builder.build()

    ds = xr.open_zarr(store, consolidated=False, zarr_format=3, group="sfc")

    assert set(ds.data_vars) == {"wind_u", "wind_v"}
    assert ds["wind_u"].dims[0] == "valid_time"
    assert ds["wind_u"].shape[0] == 12

    print("Metadata")
    print(ds.dims)
    print(ds.coords)
    print(ds.attrs)


def test_open_zarr_group_data_values_correct(read_only_fdb_pattern_setup) -> None:
    """Data values retrieved through xarray must match the expected pattern values."""
    builder = CustomStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_view(
        ["t2m"],
        "type=an,class=ea,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01/2020-01-02/2020-01-03,levtype=sfc,step=0,"
        "param=167,time=0/600/1200/1800",
        [
            AxisDefinition(["date"], Chunking.SINGLE_VALUE, dim_name="date"),
            AxisDefinition(["time"], Chunking.SINGLE_VALUE, dim_name="time"),
        ],
        ExtractorType.GRIB,
    )
    store = builder.build()

    ds = xr.open_zarr(store, consolidated=False, zarr_format=3)

    assert "t2m" in ds.data_vars
    assert ds["t2m"].dims[:2] == ("date", "time")
    # Shape: 3 dates × 4 times × N field points
    assert ds["t2m"].shape[:2] == (3, 4)

    # The fixture writes value = enumerate(product(dates, times, params)) index.
    # param=167 is the 3rd param (index 2), so value at [date_i, time_j] = date_i*4*3 + time_j*3 + 2
    arr = ds["t2m"].values
    assert np.all(arr[0, 0, :] == 2.0)  # date=0, time=0, param_idx=2 → value=2
    assert np.all(arr[0, 1, :] == 5.0)  # date=0, time=1, param_idx=2 → value=5
    assert np.all(arr[1, 0, :] == 14.0)  # date=1, time=0, param_idx=2 → value=14
