# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

"""Path-correctness tests for FdbZarrStore using a deep group hierarchy.

Each array in the tree has a distinct shape and dimensionality so that chunk
path patterns (1-D "c/N", 2-D "c/I/J", 3-D "c/I/J/K") are all exercised.

The store under test:

    root (group)                                          depth 0
    ├── arr_root  1-D  shape=(12,)   chunk=(3,)  →  4 chunks   depth 1
    ├── grp_a     (group)                                       depth 1
    │   ├── arr_a1   2-D  shape=(6,4)  chunk=(2,2)  →  6 chunks depth 2
    │   └── grp_a_inner (group)                                 depth 2
    │       ├── arr_ai1  3-D  shape=(4,3,2)  chunk=(2,3,1)  →  4 chunks  depth 3
    │       └── arr_ai2  1-D  shape=(8,)   chunk=(4,)   →  2 chunks  depth 3
    └── grp_b     (group, no direct arrays)                     depth 1
        └── grp_bb   (group, no direct arrays)                  depth 2
            └── grp_bbd  (group)                                depth 3
                └── arr_bbd  2-D  shape=(4,2)  chunk=(2,1)  →  4 chunks  depth 4

_known_paths (31 total):
     1  zarr.json
     5  arr_root        zarr.json + c/0..c/3
     1  grp_a/zarr.json
     7  grp_a/arr_a1    zarr.json + c/0/0, c/0/1, c/1/0, c/1/1, c/2/0, c/2/1
     1  grp_a/grp_a_inner/zarr.json
     5  grp_a/grp_a_inner/arr_ai1  zarr.json + c/0/0/0, c/0/0/1, c/1/0/0, c/1/0/1
     3  grp_a/grp_a_inner/arr_ai2  zarr.json + c/0, c/1
     1  grp_b/zarr.json
     1  grp_b/grp_bb/zarr.json
     1  grp_b/grp_bb/grp_bbd/zarr.json
     5  grp_b/grp_bb/grp_bbd/arr_bbd  zarr.json + c/0/0, c/0/1, c/1/0, c/1/1
"""

import json
import logging
import math

import numpy as np
import pytest
from zarr.core.buffer import default_buffer_prototype
from zarr.core.sync import _collect_aiterator, sync

from tests.z3fdb.zarr_interface_conformity._mocks import (
    ARR_A1,
    ARR_AI1,
    ARR_AI2,
    ARR_BBD,
    ARR_ROOT,
    MockChunkedDataView,
)
from z3fdb._internal.zarr import FdbZarrGroup, FdbZarrStore

log = logging.getLogger(__name__)

pytestmark = pytest.mark.offline


# ---------------------------------------------------------------------------
# Total key count and listing
# ---------------------------------------------------------------------------


def test_total_key_count(deep_store: FdbZarrStore) -> None:
    """5 arrays + 6 groups verified against expected path breakdown."""
    total = len(deep_store._known_paths)
    log.debug("total paths: %d", total)
    #  1 root zarr.json
    #  5 arr_root  (1 + 4 chunks)
    #  1 grp_a/zarr.json
    #  7 arr_a1    (1 + 6 chunks)
    #  1 grp_a_inner/zarr.json
    #  5 arr_ai1   (1 + 4 chunks)
    #  3 arr_ai2   (1 + 2 chunks)
    #  1 grp_b/zarr.json
    #  1 grp_bb/zarr.json
    #  1 grp_bbd/zarr.json
    #  5 arr_bbd   (1 + 4 chunks)
    assert total == 31


def test_list_returns_all_known_paths(deep_store: FdbZarrStore) -> None:
    keys = sync(_collect_aiterator(deep_store.list()))
    log.debug("list() returned %d keys", len(keys))
    assert set(keys) == set(deep_store._known_paths)
    assert len(keys) == 31


def test_iter_matches_list(deep_store: FdbZarrStore) -> None:
    via_iter = list(deep_store)
    via_list = sync(_collect_aiterator(deep_store.list()))
    log.debug("__iter__: %d keys, list(): %d keys", len(via_iter), len(via_list))
    assert set(via_iter) == set(via_list)


# ---------------------------------------------------------------------------
# zarr.json access — store.get(), store[], and group[] all tested per path
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "key, expected_node_type",
    [
        ("zarr.json", "group"),
        ("arr_root/zarr.json", "array"),
        ("grp_a/zarr.json", "group"),
        ("grp_a/arr_a1/zarr.json", "array"),
        ("grp_a/grp_a_inner/zarr.json", "group"),
        ("grp_a/grp_a_inner/arr_ai1/zarr.json", "array"),
        ("grp_a/grp_a_inner/arr_ai2/zarr.json", "array"),
        ("grp_b/zarr.json", "group"),
        ("grp_b/grp_bb/zarr.json", "group"),
        ("grp_b/grp_bb/grp_bbd/zarr.json", "group"),
        ("grp_b/grp_bb/grp_bbd/arr_bbd/zarr.json", "array"),
    ],
)
def test_zarr_json_accessible_via_all_apis(
    deep_store: FdbZarrStore,
    root_group: FdbZarrGroup,
    key: str,
    expected_node_type: str,
) -> None:
    """store.get(), store[], and group[] all return valid metadata for every zarr.json path."""
    for label, buf in [
        ("store.get()", sync(deep_store.get(key, prototype=default_buffer_prototype()))),
        ("store[]", sync(deep_store[key])),
        ("group[]", root_group[key]),
    ]:
        assert buf is not None, f"{label}: {key!r} returned None"
        meta = json.loads(buf.to_bytes())
        log.debug("%s %r -> node_type=%r zarr_format=%r", label, key, meta.get("node_type"), meta.get("zarr_format"))
        assert meta["zarr_format"] == 3, f"{label}: {key!r} zarr_format != 3"
        assert meta["node_type"] == expected_node_type, f"{label}: {key!r} node_type mismatch"


# ---------------------------------------------------------------------------
# Chunk access — store.get(), store[], and group[] all tested per chunk
# ---------------------------------------------------------------------------


def _expected_flat_index(chunks_per_axis: tuple, chunk_index: tuple) -> int:
    return int(sum(idx * math.prod(chunks_per_axis[i + 1 :]) for i, idx in enumerate(chunk_index)))


@pytest.mark.parametrize(
    "key, cfg, chunk_index",
    [
        # arr_root: 1-D, chunk_shape=(3,), 3 float32 → 12 bytes per chunk
        ("arr_root/c/0", ARR_ROOT, (0,)),
        ("arr_root/c/3", ARR_ROOT, (3,)),
        # arr_a1: 2-D, chunk_shape=(2,2), 4 float32 → 16 bytes per chunk
        ("grp_a/arr_a1/c/0/0", ARR_A1, (0, 0)),
        ("grp_a/arr_a1/c/2/1", ARR_A1, (2, 1)),
        # arr_ai1: 3-D, chunk_shape=(2,3,1), 6 float32 → 24 bytes per chunk
        ("grp_a/grp_a_inner/arr_ai1/c/0/0/0", ARR_AI1, (0, 0, 0)),
        ("grp_a/grp_a_inner/arr_ai1/c/1/0/1", ARR_AI1, (1, 0, 1)),
        # arr_ai2: 1-D, chunk_shape=(4,), 4 float32 → 16 bytes per chunk
        ("grp_a/grp_a_inner/arr_ai2/c/0", ARR_AI2, (0,)),
        ("grp_a/grp_a_inner/arr_ai2/c/1", ARR_AI2, (1,)),
        # arr_bbd: 2-D, chunk_shape=(2,1), 2 float32 → 8 bytes per chunk
        ("grp_b/grp_bb/grp_bbd/arr_bbd/c/0/0", ARR_BBD, (0, 0)),
        ("grp_b/grp_bb/grp_bbd/arr_bbd/c/1/1", ARR_BBD, (1, 1)),
    ],
)
def test_chunk_accessible_via_all_apis(
    deep_store: FdbZarrStore,
    root_group: FdbZarrGroup,
    key: str,
    cfg: tuple,
    chunk_index: tuple,
) -> None:
    """store.get(), store[], and group[] all return correct byte size and chunk content."""
    shape, chunk_shape, chunks_per_axis = cfg
    mock = MockChunkedDataView(shape, chunk_shape, chunks_per_axis)
    expected_bytes = mock.chunk_bytes()
    expected_flat = _expected_flat_index(chunks_per_axis, chunk_index)

    for label, buf in [
        ("store.get()", sync(deep_store.get(key, prototype=default_buffer_prototype()))),
        ("store[]", sync(deep_store[key])),
        ("group[]", root_group[key]),
    ]:
        assert buf is not None, f"{label}: {key!r} returned None"
        raw = buf.to_bytes()
        log.debug("%s %r -> %d bytes, flat_chunk_index=%d", label, key, len(raw), expected_flat)
        assert len(raw) == expected_bytes, f"{label}: {key!r} expected {expected_bytes} bytes, got {len(raw)}"
        values = np.frombuffer(raw, dtype="<f4")
        assert all(v == float(expected_flat) for v in values), (
            f"{label}: {key!r} expected all values == {float(expected_flat)}, got {values}"
        )


# ---------------------------------------------------------------------------
# get() — nonexistent paths return None
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "key",
    [
        "nonexistent.json",
        "grp_a/nonexistent/zarr.json",
        "grp_b/arr_bbd/zarr.json",  # arr_bbd is deeper than grp_b
        "arr_root/c/99",  # chunk index out of range
        "grp_b/grp_bb/grp_bbd/c/0",  # grp_bbd is a group, not an array
        "c/0",  # chunk without array prefix
    ],
)
def test_get_nonexistent_returns_none(deep_store: FdbZarrStore, key: str) -> None:
    buf = sync(deep_store.get(key, prototype=default_buffer_prototype()))
    log.debug("get(%r) -> %r", key, buf)
    assert buf is None, f"expected None for {key!r}, got {type(buf).__name__}"


# ---------------------------------------------------------------------------
# store[] — missing paths raise KeyError
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "key",
    [
        "does_not_exist",
        "grp_a/does_not_exist/zarr.json",
        "arr_root/c/99",
    ],
)
def test_store_getitem_missing_raises_key_error(deep_store: FdbZarrStore, key: str) -> None:
    """store[key] raises KeyError for unknown paths (unlike get() which returns None)."""
    log.debug("expecting KeyError for store[%r]", key)
    with pytest.raises(KeyError):
        sync(deep_store[key])


# ---------------------------------------------------------------------------
# group[] — missing paths raise KeyError
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "key",
    [
        "does_not_exist",
        "grp_a/does_not_exist/zarr.json",
        "grp_b/arr_bbd/zarr.json",  # arr_bbd is under grp_bbd, not grp_b
    ],
)
def test_group_getitem_missing_raises_key_error(root_group: FdbZarrGroup, key: str) -> None:
    log.debug("expecting KeyError for group[%r]", key)
    with pytest.raises(KeyError):
        root_group[key]


# ---------------------------------------------------------------------------
# __contains__ and exists()
# ---------------------------------------------------------------------------


def test_contains_all_known_paths(deep_store: FdbZarrStore) -> None:
    for key in deep_store._known_paths:
        assert key in deep_store, f"{key!r} should be in store"
    log.debug("all %d known paths confirmed present", len(deep_store._known_paths))


@pytest.mark.parametrize(
    "key",
    [
        "",
        "nonexistent",
        "grp_a/zarr.json/extra",
        "grp_b/arr_bbd/zarr.json",
        "c/0",
        "arr_root/c/4",  # one past last chunk
    ],
)
def test_contains_rejects_unknown_paths(deep_store: FdbZarrStore, key: str) -> None:
    result = key in deep_store
    log.debug("%r in store -> %r", key, result)
    assert not result


def test_exists_all_known_paths(deep_store: FdbZarrStore) -> None:
    for key in deep_store._known_paths:
        assert sync(deep_store.exists(key)), f"exists({key!r}) returned False"
    log.debug("exists() confirmed for all %d paths", len(deep_store._known_paths))


def test_exists_unknown_path(deep_store: FdbZarrStore) -> None:
    key = "grp_a/does_not_exist/zarr.json"
    result = sync(deep_store.exists(key))
    log.debug("exists(%r) -> %r", key, result)
    assert not result


# ---------------------------------------------------------------------------
# list_prefix — subtree access
# ---------------------------------------------------------------------------


def test_list_prefix_empty_returns_all(deep_store: FdbZarrStore) -> None:
    keys = sync(_collect_aiterator(deep_store.list_prefix("")))
    log.debug("list_prefix('') -> %d keys", len(keys))
    assert set(keys) == set(deep_store._known_paths)


@pytest.mark.parametrize(
    "prefix, expected_count",
    [
        # grp_a: zarr.json(1) + arr_a1(7) + grp_a_inner zarr.json(1) + arr_ai1(5) + arr_ai2(3)
        ("grp_a/", 17),
        # grp_a_inner: zarr.json(1) + arr_ai1(5) + arr_ai2(3)
        ("grp_a/grp_a_inner/", 9),
        # grp_b: grp_b zarr.json(1) + grp_bb zarr.json(1) + grp_bbd zarr.json(1) + arr_bbd(5)
        ("grp_b/", 8),
        ("grp_b/grp_bb/", 7),
        ("grp_b/grp_bb/grp_bbd/", 6),
        ("grp_b/grp_bb/grp_bbd/arr_bbd/", 5),
    ],
)
def test_list_prefix_subtree_count(deep_store: FdbZarrStore, prefix: str, expected_count: int) -> None:
    keys = sync(_collect_aiterator(deep_store.list_prefix(prefix)))
    log.debug("list_prefix(%r) -> %d keys: %s", prefix, len(keys), sorted(keys))
    assert all(k.startswith(prefix) for k in keys)
    assert len(keys) == expected_count, (
        f"list_prefix({prefix!r}): expected {expected_count}, got {len(keys)}\n"
        + "\n".join(f"  {k}" for k in sorted(keys))
    )


# ---------------------------------------------------------------------------
# list_prefix + suffix filter (suffix-based access)
# ---------------------------------------------------------------------------


def test_suffix_access_all_zarr_json(deep_store: FdbZarrStore) -> None:
    """All 11 metadata nodes found by filtering list() on the zarr.json suffix."""
    all_keys = sync(_collect_aiterator(deep_store.list_prefix("")))
    meta_keys = [k for k in all_keys if k.endswith("zarr.json")]
    log.debug("zarr.json suffix: %d keys: %s", len(meta_keys), sorted(meta_keys))
    expected = {
        "zarr.json",
        "arr_root/zarr.json",
        "grp_a/zarr.json",
        "grp_a/arr_a1/zarr.json",
        "grp_a/grp_a_inner/zarr.json",
        "grp_a/grp_a_inner/arr_ai1/zarr.json",
        "grp_a/grp_a_inner/arr_ai2/zarr.json",
        "grp_b/zarr.json",
        "grp_b/grp_bb/zarr.json",
        "grp_b/grp_bb/grp_bbd/zarr.json",
        "grp_b/grp_bb/grp_bbd/arr_bbd/zarr.json",
    }
    assert set(meta_keys) == expected


def test_suffix_access_all_chunks(deep_store: FdbZarrStore) -> None:
    """All 20 chunk paths found by filtering on '/c/' in the key."""
    all_keys = sync(_collect_aiterator(deep_store.list_prefix("")))
    chunk_keys = [k for k in all_keys if "/c/" in k]
    log.debug("chunk filter: %d keys", len(chunk_keys))
    # arr_root(4) + arr_a1(6) + arr_ai1(4) + arr_ai2(2) + arr_bbd(4) = 20
    assert len(chunk_keys) == 20
    assert all("/c/" in k for k in chunk_keys)


def test_suffix_access_chunk_dimensionality(deep_store: FdbZarrStore) -> None:
    """1-D chunks have the form 'c/N', 2-D 'c/I/J', 3-D 'c/I/J/K'."""
    all_keys = sync(_collect_aiterator(deep_store.list_prefix("")))
    chunk_keys = [k for k in all_keys if "/c/" in k]

    def chunk_dims(key: str) -> int:
        c_part = key[key.index("/c/") + 1 :]  # e.g. "c/0/1"
        return len(c_part.split("/")) - 1  # subtract the leading "c"

    dims = {k: chunk_dims(k) for k in chunk_keys}
    log.debug("chunk dimensionalities: %s", dict(sorted(dims.items())))

    one_d = [k for k, d in dims.items() if d == 1]
    two_d = [k for k, d in dims.items() if d == 2]
    three_d = [k for k, d in dims.items() if d == 3]

    log.debug("1-D chunks (%d): %s", len(one_d), sorted(one_d))
    log.debug("2-D chunks (%d): %s", len(two_d), sorted(two_d))
    log.debug("3-D chunks (%d): %s", len(three_d), sorted(three_d))

    assert len(one_d) == 6  # arr_root(4) + arr_ai2(2)
    assert len(two_d) == 10  # arr_a1(6) + arr_bbd(4)
    assert len(three_d) == 4  # arr_ai1(4)


def test_suffix_access_subtree_zarr_json(deep_store: FdbZarrStore) -> None:
    """Combining list_prefix with a suffix filter scopes metadata to a subtree."""
    subtree = sync(_collect_aiterator(deep_store.list_prefix("grp_b/")))
    meta = [k for k in subtree if k.endswith("zarr.json")]
    log.debug("grp_b subtree zarr.json: %s", sorted(meta))
    assert set(meta) == {
        "grp_b/zarr.json",
        "grp_b/grp_bb/zarr.json",
        "grp_b/grp_bb/grp_bbd/zarr.json",
        "grp_b/grp_bb/grp_bbd/arr_bbd/zarr.json",
    }


def test_suffix_access_groups_only_branch_has_no_chunks(deep_store: FdbZarrStore) -> None:
    """The grp_b branch has chunks only at arr_bbd; intermediate groups add none."""
    bbd_keys = sync(_collect_aiterator(deep_store.list_prefix("grp_b/grp_bb/grp_bbd/")))
    chunk_keys = [k for k in bbd_keys if "/c/" in k]
    log.debug("grp_bbd subtree chunks: %s", sorted(chunk_keys))
    assert all(k.startswith("grp_b/grp_bb/grp_bbd/arr_bbd/c/") for k in chunk_keys)
    assert len(chunk_keys) == 4  # 2×2 = 4 chunks


# ---------------------------------------------------------------------------
# list_dir — immediate children at every depth
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "prefix, expected_entries",
    [
        ("", {"zarr.json", "arr_root", "grp_a", "grp_b"}),
        ("arr_root", {"zarr.json", "c"}),
        ("grp_a", {"zarr.json", "arr_a1", "grp_a_inner"}),
        ("grp_a/arr_a1", {"zarr.json", "c"}),
        ("grp_a/grp_a_inner", {"zarr.json", "arr_ai1", "arr_ai2"}),
        ("grp_a/grp_a_inner/arr_ai1", {"zarr.json", "c"}),
        ("grp_b", {"zarr.json", "grp_bb"}),
        ("grp_b/grp_bb", {"zarr.json", "grp_bbd"}),
        ("grp_b/grp_bb/grp_bbd", {"zarr.json", "arr_bbd"}),
        ("grp_b/grp_bb/grp_bbd/arr_bbd", {"zarr.json", "c"}),
    ],
)
def test_list_dir(deep_store: FdbZarrStore, prefix: str, expected_entries: set) -> None:
    entries = set(sync(_collect_aiterator(deep_store.list_dir(prefix))))
    log.debug("list_dir(%r) -> %s", prefix, sorted(entries))
    assert entries == expected_entries, (
        f"list_dir({prefix!r}): expected {sorted(expected_entries)}, got {sorted(entries)}"
    )
    assert all("/" not in e for e in entries), (
        f"list_dir({prefix!r}) returned entries with '/': {[e for e in entries if '/' in e]}"
    )


def test_list_dir_nonexistent_prefix_returns_empty(deep_store: FdbZarrStore) -> None:
    entries = sync(_collect_aiterator(deep_store.list_dir("does_not_exist")))
    log.debug("list_dir('does_not_exist') -> %s", entries)
    assert not entries
