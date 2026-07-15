"""Tests for xarray compatibility — Phase 1: dimension_names in zarr metadata.

These tests verify that:
- AxisDefinition auto-derives dimension names from its keys.
- User-provided dim_name overrides the auto-derived value.
- Both SimpleStoreBuilder and CustomStoreBuilder propagate dimension_names
  into each array's zarr.json.
- The final (field-values) dimension is always None.
"""
import json

from z3fdb import (
    AxisDefinition,
    Chunking,
    ExtractorType,
    SimpleStoreBuilder,
)
from z3fdb.custom_store_builder import CustomStoreBuilder


# ---------------------------------------------------------------------------
# Unit tests for AxisDefinition.dim_name
# ---------------------------------------------------------------------------


class TestAxisDefinitionDimName:
    """AxisDefinition must auto-derive dim_name and respect user overrides."""

    def test_single_key_auto_derives_name(self):
        ax = AxisDefinition(["param"], Chunking.SINGLE_VALUE)
        assert ax.dim_name == "param"

    def test_multi_key_auto_derives_joined_name(self):
        ax = AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE)
        assert ax.dim_name == "date_time"

    def test_user_provided_name_is_used(self):
        ax = AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE, dim_name="valid_time")
        assert ax.dim_name == "valid_time"

    def test_user_provided_name_overrides_auto(self):
        ax = AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE, dim_name="my_dim")
        assert ax.dim_name == "my_dim"
        assert ax.dim_name != "date_time"

    def test_dim_name_setter(self):
        ax = AxisDefinition(["step"], Chunking.SINGLE_VALUE)
        ax.dim_name = "forecast_step"
        assert ax.dim_name == "forecast_step"

    def test_none_dim_name_causes_auto_derivation(self):
        ax = AxisDefinition(["step"], Chunking.SINGLE_VALUE, dim_name=None)
        assert ax.dim_name == "step"


# ---------------------------------------------------------------------------
# Integration tests for SimpleStoreBuilder dimension_names in zarr.json
# ---------------------------------------------------------------------------


def _read_zarr_json(store, path="zarr.json") -> dict:
    import asyncio
    async def _get():
        buf = await store.get(path)
        return json.loads(buf.to_bytes())
    return asyncio.run(_get())


def test_simple_store_dimension_names_in_zarr_json(read_only_fdb_pattern_setup) -> None:
    """SimpleStoreBuilder must write dimension_names into zarr.json."""
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        "type=an,class=ea,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01/2020-01-02,levtype=sfc,step=0,"
        "param=167,time=0/600/1200/1800",
        [
            AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )
    store = builder.build()
    meta = _read_zarr_json(store)
    assert "dimension_names" in meta
    # auto-derived: date_time, param, None (field values)
    assert meta["dimension_names"] == ["date_time", "param", None]


def test_simple_store_user_dim_names_in_zarr_json(read_only_fdb_pattern_setup) -> None:
    """User-supplied dim_name values must appear verbatim in zarr.json."""
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        "type=an,class=ea,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01/2020-01-02,levtype=sfc,step=0,"
        "param=167,time=0/600/1200/1800",
        [
            AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE, dim_name="valid_time"),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE, dim_name="parameter"),
        ],
        ExtractorType.GRIB,
    )
    store = builder.build()
    meta = _read_zarr_json(store)
    assert meta["dimension_names"] == ["valid_time", "parameter", None]


def test_simple_store_field_dimension_is_none(read_only_fdb_pattern_setup) -> None:
    """The implicit field-values dimension must always be None."""
    builder = SimpleStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        "type=an,class=ea,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01,levtype=sfc,step=0,param=167,time=0",
        [AxisDefinition(["date"], Chunking.SINGLE_VALUE)],
        ExtractorType.GRIB,
    )
    store = builder.build()
    meta = _read_zarr_json(store)
    assert meta["dimension_names"][-1] is None


# ---------------------------------------------------------------------------
# Integration tests for CustomStoreBuilder dimension_names
# ---------------------------------------------------------------------------


def test_custom_store_dimension_names_in_zarr_json(read_only_fdb_pattern_setup) -> None:
    """CustomStoreBuilder must write dimension_names into each array's zarr.json."""
    builder = CustomStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_view(
        ["group_a", "my_array"],
        "type=an,class=ea,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01/2020-01-02,levtype=sfc,step=0,param=167/165,"
        "time=0/600/1200/1800",
        [
            AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE, dim_name="valid_time"),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )
    store = builder.build()
    meta = _read_zarr_json(store, "group_a/my_array/zarr.json")
    assert "dimension_names" in meta
    assert meta["dimension_names"] == ["valid_time", "param", None]


def test_custom_store_multiple_arrays_independent_dim_names(read_only_fdb_pattern_setup) -> None:
    """Each array in a CustomStoreBuilder can have its own dimension names."""
    builder = CustomStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_view(
        ["arr_1"],
        "type=an,class=ea,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01,levtype=sfc,step=0,param=167,time=0/600/1200/1800",
        [AxisDefinition(["time"], Chunking.SINGLE_VALUE, dim_name="t")],
        ExtractorType.GRIB,
    )
    builder.add_view(
        ["arr_2"],
        "type=an,class=ea,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01,levtype=sfc,step=0,param=165/166,time=0",
        [AxisDefinition(["param"], Chunking.SINGLE_VALUE, dim_name="p")],
        ExtractorType.GRIB,
    )
    store = builder.build()

    meta_1 = _read_zarr_json(store, "arr_1/zarr.json")
    meta_2 = _read_zarr_json(store, "arr_2/zarr.json")

    assert meta_1["dimension_names"] == ["t", None]
    assert meta_2["dimension_names"] == ["p", None]

