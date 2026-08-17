# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Tests for zarr v3 consolidated metadata embedded in the root zarr.json.

Simple store tests build stores inline; deep hierarchy tests use the shared
``deep_store`` fixture from conftest (see _mocks.build_deep_store for the tree).
"""

import json

import pytest
import zarr
from zarr.core.buffer import default_buffer_prototype
from zarr.core.sync import sync

from tests.z3fdb.zarr_interface_conformity._mocks import MockChunkedDataView, make_array
from z3fdb._internal.zarr import FdbSource, FdbZarrArray, FdbZarrGroup, FdbZarrStore

pytestmark = pytest.mark.offline


def _flat_metadata(store):
    """Return the consolidated_metadata.metadata dict from the root zarr.json."""
    buf = sync(store.get("zarr.json", prototype=default_buffer_prototype()))
    return json.loads(buf.to_bytes())["consolidated_metadata"]["metadata"]


# ---------------------------------------------------------------------------
# Simple inline stores
# ---------------------------------------------------------------------------


def test_consolidated_metadata_flat_group():
    """Root group with two array children: flat metadata with keys {a, b}."""
    store = FdbZarrStore(FdbZarrGroup(name="", children=[make_array("a"), make_array("b")]))
    buf = sync(store.get("zarr.json", prototype=default_buffer_prototype()))
    root = json.loads(buf.to_bytes())
    assert root["zarr_format"] == 3
    consolidated_metadata = root["consolidated_metadata"]
    assert consolidated_metadata["kind"] == "inline"
    assert consolidated_metadata["must_understand"] is False
    metadata = consolidated_metadata["metadata"]
    assert set(metadata.keys()) == {"a", "b"}
    assert all(metadata[k]["node_type"] == "array" for k in ("a", "b"))
    assert "a/zarr.json" not in metadata  # keys must not carry the /zarr.json suffix


def test_consolidated_metadata_nested_group():
    """Nested group's consolidated_metadata uses relative paths; values carry no nested metadata."""
    inner = FdbZarrGroup(name="inner", children=[make_array("c")])
    store = FdbZarrStore(FdbZarrGroup(name="", children=[make_array("a"), inner]))
    metadata = _flat_metadata(store)
    assert set(metadata.keys()) == {"a", "inner", "inner/c"}
    inner_consolidated_metadata = metadata["inner"]["consolidated_metadata"]
    assert inner_consolidated_metadata["kind"] == "inline"
    assert set(inner_consolidated_metadata["metadata"].keys()) == {"c"}
    assert "consolidated_metadata" not in inner_consolidated_metadata["metadata"]["c"]


def test_consolidated_metadata_sort_order():
    """Shallower nodes appear before deeper; same depth is alphabetical."""
    inner = FdbZarrGroup(name="inner", children=[make_array("z"), make_array("a")])
    store = FdbZarrStore(FdbZarrGroup(name="", children=[make_array("m"), inner]))
    keys = list(_flat_metadata(store).keys())
    depths = [k.count("/") for k in keys]
    assert depths == sorted(depths)
    for depth in range(2):
        at_depth = [k for k in keys if k.count("/") == depth]
        assert at_depth == sorted(at_depth)


def test_consolidated_metadata_array_root_has_no_consolidated_metadata():
    """Array-rooted store has no consolidated_metadata in zarr.json."""
    shape, chunk_shape, chunks_per_axis = (4,), (1,), (4,)
    store = FdbZarrStore(
        FdbZarrArray(name="", datasource=FdbSource(MockChunkedDataView(shape, chunk_shape, chunks_per_axis)))
    )
    buf = sync(store.get("zarr.json", prototype=default_buffer_prototype()))
    assert buf
    root = json.loads(buf.to_bytes())
    assert "consolidated_metadata" not in root
    assert root["node_type"] == "array"


def test_zmetadata_key_not_present():
    """.zmetadata is a zarr v2 artifact and must not be served by v3 stores."""
    store = FdbZarrStore(FdbZarrGroup(name="", children=[make_array("a")]))
    assert not sync(store.exists(".zmetadata"))
    assert sync(store.get(".zmetadata", prototype=default_buffer_prototype())) is None


# ---------------------------------------------------------------------------
# Deep hierarchy — uses the shared deep_store fixture (see conftest.py)
# ---------------------------------------------------------------------------

_DEEP_ARRAYS = {
    "arr_root",
    "grp_a/arr_a1",
    "grp_a/grp_a_inner/arr_ai1",
    "grp_a/grp_a_inner/arr_ai2",
    "grp_b/grp_bb/grp_bbd/arr_bbd",
}
_DEEP_GROUPS = {
    "grp_a": {"arr_a1", "grp_a_inner", "grp_a_inner/arr_ai1", "grp_a_inner/arr_ai2"},
    "grp_b": {"grp_bb", "grp_bb/grp_bbd", "grp_bb/grp_bbd/arr_bbd"},
    "grp_a/grp_a_inner": {"arr_ai1", "arr_ai2"},
    "grp_b/grp_bb": {"grp_bbd", "grp_bbd/arr_bbd"},
    "grp_b/grp_bb/grp_bbd": {"arr_bbd"},
}


def test_consolidated_metadata_deep_key_set(deep_store):
    """Flat metadata contains exactly the 10 expected node paths."""
    assert set(_flat_metadata(deep_store).keys()) == _DEEP_ARRAYS | set(_DEEP_GROUPS)


def test_consolidated_metadata_deep_sort_order(deep_store):
    """Shallower nodes before deeper; alphabetical within depth."""
    keys = list(_flat_metadata(deep_store).keys())
    depths = [k.count("/") for k in keys]
    assert depths == sorted(depths)
    for depth in range(5):
        at_depth = [k for k in keys if k.count("/") == depth]
        assert at_depth == sorted(at_depth)


@pytest.mark.parametrize("path", sorted(_DEEP_ARRAYS))
def test_consolidated_metadata_deep_array_node(deep_store, path):
    """Array node has node_type='array' and no consolidated_metadata."""
    node = _flat_metadata(deep_store)[path]
    assert node["node_type"] == "array"
    assert "consolidated_metadata" not in node


@pytest.mark.parametrize("path, expected_nested", sorted(_DEEP_GROUPS.items()))
def test_consolidated_metadata_deep_group_node(deep_store, path, expected_nested):
    """Group's nested consolidated_metadata covers all descendants (relative paths)."""
    node = _flat_metadata(deep_store)[path]
    assert node["node_type"] == "group"
    consolidated_metadata = node["consolidated_metadata"]
    assert consolidated_metadata["kind"] == "inline"
    assert consolidated_metadata["must_understand"] is False
    assert set(consolidated_metadata["metadata"].keys()) == expected_nested
    assert all("consolidated_metadata" not in v for v in consolidated_metadata["metadata"].values())


def test_consolidated_metadata_zarr_api(deep_store):
    """zarr.open_group reads consolidated metadata and navigates nested nodes."""
    root_group = zarr.open_group(deep_store, mode="r", zarr_format=3)
    assert root_group.metadata
    array = root_group["grp_a/grp_a_inner/arr_ai1"]
    assert array.metadata
    deep_group = root_group["grp_a/grp_a_inner"]
    assert deep_group.metadata
    assert deep_group.metadata.consolidated_metadata
