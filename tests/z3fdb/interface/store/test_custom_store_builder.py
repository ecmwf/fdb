# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0
"""Tests for CustomStoreBuilder: hierarchy construction, repeated parts, extension, and
cross-type name-collision detection.

Fixture data (from conftest build_pattern_grib_messages):
  dates      = [20200101, 20200102, 20200103]   (3 dates, index d)
  times      = [0, 600, 1200, 1800]             (4 times, index t)
  params_sfc = [165, 166, 167]                  (3 params, index p)
  params_pl  = [131, 132, 133]                  (3 params, index p)
  levels     = [50, 100, 150]                   (3 levels, index l)

Field-value formulas (all indices 0-based):
  sfc_value(d, t, p)    = d*12 + t*3 + p          (sequential over product(dates,times,params_sfc))
  pl_value(d, t, p, l)  = 36 + d*36 + t*9 + p*3 + l
"""

import numpy as np
import pytest
import zarr

from z3fdb import AxisDefinition, Chunking, CustomStoreBuilder, ExtractorType

pytestmark = [pytest.mark.offline, pytest.mark.zfdb_user_tests]

# ---------------------------------------------------------------------------
# Shared request helpers
# ---------------------------------------------------------------------------

_COMMON = {
    "type": "an",
    "class": "ea",
    "domain": "g",
    "expver": "0001",
    "stream": "oper",
    "step": 0,
}

_SFC_REQUEST = {
    **_COMMON,
    "levtype": "sfc",
    "date": ["2020-01-01", "2020-01-02", "2020-01-03"],
    "time": [0, 600, 1200, 1800],
    "param": [165, 166, 167],
}

_SFC_AXES = [
    AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
    AxisDefinition(["param"], Chunking.SINGLE_VALUE),
]

_PL_REQUEST = {
    **_COMMON,
    "levtype": "pl",
    "date": ["2020-01-01", "2020-01-02", "2020-01-03"],
    "time": [0, 600, 1200, 1800],
    "param": [131, 132, 133],
    "levelist": [50, 100, 150],
}

_PL_AXES = [
    AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
    AxisDefinition(["param", "levelist"], Chunking.SINGLE_VALUE),
]


# Structural / data tests  (require read_only_fdb_pattern_setup)
def test_single_group_single_array(read_only_fdb_pattern_setup) -> None:
    """One group containing one array — smoke test for the minimal hierarchy.

    Verifies:
    - zarr can navigate into the group and open the array.
    - Array shape reflects 12 date×time entries, 3 params, plus the implicit axis.
    - Spot-check values match the known sfc_value formula.
    """
    builder = CustomStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part("sfc/wind", _SFC_REQUEST, _SFC_AXES, ExtractorType.Grib())
    store = builder.build()

    root = zarr.open_group(store, mode="r")
    assert "sfc" in root, "top-level group 'sfc' missing from store"
    arr = root["sfc"]["wind"]

    assert arr.shape[:2] == (12, 3), f"unexpected shape {arr.shape}"

    # sfc_value(d=0, t=0, p=0) = 0
    assert np.all(arr[0, 0] == 0), f"unexpected value at [0,0]: {arr[0, 0, 0]}"
    # sfc_value(d=2, t=3, p=2) = 24+9+2 = 35
    assert np.all(arr[11, 2] == 35), f"unexpected value at [11,2]: {arr[11, 2, 0]}"


def test_sibling_arrays_same_group(read_only_fdb_pattern_setup) -> None:
    """Two sibling arrays under one parent group — both must be accessible and correct.

    'sfc' carries 3 params (shape (12, 3, N)).
    'pl' carries 3 params × 3 levels combined (shape (12, 9, N)).
    """
    builder = CustomStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part("weather/sfc", _SFC_REQUEST, _SFC_AXES, ExtractorType.Grib())
    builder.add_part("weather/pl", _PL_REQUEST, _PL_AXES, ExtractorType.Grib())
    store = builder.build()

    root = zarr.open_group(store, mode="r")
    weather = root["weather"]

    assert "sfc" in weather, "'sfc' array missing from 'weather' group"
    assert "pl" in weather, "'pl' array missing from 'weather' group"

    sfc = weather["sfc"]
    pl = weather["pl"]

    assert sfc.shape[:2] == (12, 3), f"sfc shape {sfc.shape}"
    assert pl.shape[:2] == (12, 9), f"pl shape {pl.shape}"

    # sfc spot-check: sfc_value(d=0, t=0, p=0) = 0
    assert np.all(sfc[0, 0] == 0)
    # sfc spot-check: sfc_value(d=2, t=3, p=2) = 35
    assert np.all(sfc[11, 2] == 35)

    # pl spot-check: pl_value(d=0, t=0, p=0, l=0) = 36
    # combined pl_idx = p*3+l = 0
    assert np.all(pl[0, 0] == 36)
    # pl spot-check: pl_value(d=2, t=3, p=2, l=2) = 36+72+27+6+2 = 143
    # combined dt_idx=11, pl_idx=8
    assert np.all(pl[11, 8] == 143)


def test_deep_nesting(read_only_fdb_pattern_setup) -> None:
    """Arrays at depth-3 paths sharing a common grandparent group.

    Path A: ["analysis", "sfc", "wind"]
    Path B: ["analysis", "pl",  "wind"]

    Both arrays must be accessible; group "analysis" must contain exactly
    the two sub-groups "sfc" and "pl".
    """
    builder = CustomStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part("analysis/sfc/wind", _SFC_REQUEST, _SFC_AXES, ExtractorType.Grib())
    builder.add_part("analysis/pl/wind", _PL_REQUEST, _PL_AXES, ExtractorType.Grib())
    store = builder.build()

    root = zarr.open_group(store, mode="r")
    assert "analysis" in root

    sfc_wind = root["analysis"]["sfc"]["wind"]
    pl_wind = root["analysis"]["pl"]["wind"]

    assert sfc_wind.shape[:2] == (12, 3)
    assert pl_wind.shape[:2] == (12, 9)

    assert np.all(sfc_wind[0, 0] == 0)
    assert np.all(pl_wind[0, 0] == 36)


def test_repeated_add_part_multi_part(read_only_fdb_pattern_setup) -> None:
    """Calling add_part twice with the same path registers two parts on one builder.

    Part 1: param=165 → param-axis index 0 → values = d*12 + t*3 + 0
    Part 2: param=166 → param-axis index 1 → values = d*12 + t*3 + 1

    extend_on_axis(path, 1) declares that the param axis is the extension axis.
    The assembled array must have shape (12, 2, N).
    """
    path = "data/wind"
    axes_one_param = [
        AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
        AxisDefinition(["param"], Chunking.SINGLE_VALUE),
    ]

    builder = CustomStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        path,
        {
            **_COMMON,
            "levtype": "sfc",
            "date": ["2020-01-01", "2020-01-02", "2020-01-03"],
            "time": [0, 600, 1200, 1800],
            "param": [165],
        },
        axes_one_param,
        ExtractorType.Grib(),
    )
    builder.extend_on_axis(path, 1)
    builder.add_part(
        path,
        {
            **_COMMON,
            "levtype": "sfc",
            "date": ["2020-01-01", "2020-01-02", "2020-01-03"],
            "time": [0, 600, 1200, 1800],
            "param": [166],
        },
        axes_one_param,
        ExtractorType.Grib(),
    )
    store = builder.build()

    root = zarr.open_group(store, mode="r")
    arr = root["data"]["wind"]

    assert arr.shape[:2] == (12, 2), f"expected (12, 2, N), got {arr.shape}"

    # param=165 is axis-1 index 0; sfc_value(d=0,t=0,p=0) = 0
    assert np.all(arr[0, 0] == 0)
    # param=166 is axis-1 index 1; sfc_value(d=0,t=0,p=1) = 1
    assert np.all(arr[0, 1] == 1)
    # sfc_value(d=2,t=3,p=0) = 33 and sfc_value(d=2,t=3,p=1) = 34
    assert np.all(arr[11, 0] == 33)
    assert np.all(arr[11, 1] == 34)


def test_extend_on_axis_shape(read_only_fdb_pattern_setup) -> None:
    """extend_on_axis via CustomStoreBuilder correctly stitches two SFC + PL parts.

    SFC part: 3 params  → axis-1 indices 0..2
    PL  part: 9 (param×level) → axis-1 indices 3..11

    Both parts share the same date×time axis (12 entries).
    """
    path = "combined/all"
    dt_ax = AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE)

    builder = CustomStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        path,
        _SFC_REQUEST,
        [dt_ax, AxisDefinition(["param"], Chunking.SINGLE_VALUE)],
        ExtractorType.Grib(),
    )
    builder.extend_on_axis(path, 1)
    builder.add_part(
        path,
        _PL_REQUEST,
        [dt_ax, AxisDefinition(["param", "levelist"], Chunking.SINGLE_VALUE)],
        ExtractorType.Grib(),
    )
    store = builder.build()

    arr = zarr.open_group(store, mode="r")["combined"]["all"]

    assert arr.shape[:2] == (12, 12), f"expected (12, 12, N), got {arr.shape}"

    # SFC values occupy axis-1 indices 0..2
    assert np.all(arr[0, 0] == 0)  # sfc_value(0,0,0)
    assert np.all(arr[11, 2] == 35)  # sfc_value(2,3,2)

    # PL values occupy axis-1 indices 3..11
    assert np.all(arr[0, 3] == 36)  # pl_value(0,0,0,0)
    assert np.all(arr[11, 11] == 143)  # pl_value(2,3,2,2)


# Collision detection tests  (no FDB access needed — errors fire before build())
_DUMMY_REQUEST = {"type": "an", "class": "ea"}
_DUMMY_AXES: list[AxisDefinition] = []
_DUMMY_EXTRACTOR = ExtractorType.Grib()


def test_array_then_group_collision_raises() -> None:
    """Registering an array at 'foo' and then requesting 'foo/bar' must raise ValueError.

    The first call registers 'foo' as a leaf array under the root group.
    The second call attempts to treat 'foo' as a parent group — a cross-type collision.
    """
    builder = CustomStoreBuilder()
    builder.add_part("foo", _DUMMY_REQUEST, _DUMMY_AXES, _DUMMY_EXTRACTOR)
    with pytest.raises(ValueError, match="foo"):
        builder.add_part("foo/bar", _DUMMY_REQUEST, _DUMMY_AXES, _DUMMY_EXTRACTOR)


def test_group_then_array_collision_raises() -> None:
    """Registering 'foo/bar' and then requesting 'foo' as an array must raise ValueError.

    The first call creates an intermediate group named 'foo'.
    The second call attempts to register an array also named 'foo' — a cross-type collision.
    """
    builder = CustomStoreBuilder()
    builder.add_part("foo/bar", _DUMMY_REQUEST, _DUMMY_AXES, _DUMMY_EXTRACTOR)
    with pytest.raises(ValueError, match="foo"):
        builder.add_part("foo", _DUMMY_REQUEST, _DUMMY_AXES, _DUMMY_EXTRACTOR)


def test_deeply_nested_collision_raises() -> None:
    """Cross-type collision detected at a non-root level.

    Registers 'a/b/arr', making 'a' and 'b' intermediate groups.
    Then tries to register 'a/b' as an array — 'b' is already a group.
    """
    builder = CustomStoreBuilder()
    builder.add_part("a/b/arr", _DUMMY_REQUEST, _DUMMY_AXES, _DUMMY_EXTRACTOR)
    with pytest.raises(ValueError, match="b"):
        builder.add_part("a/b", _DUMMY_REQUEST, _DUMMY_AXES, _DUMMY_EXTRACTOR)


def test_empty_path_raises() -> None:
    """An empty path string must raise ValueError before any tree mutation."""
    builder = CustomStoreBuilder()
    with pytest.raises(ValueError, match="empty"):
        builder.add_part("", _DUMMY_REQUEST, _DUMMY_AXES, _DUMMY_EXTRACTOR)


def test_root_array(read_only_fdb_pattern_setup) -> None:
    """path=None places the array at the store root — no enclosing group.

    The store must be openable as a zarr array directly via
    ``zarr.open_array(store)`` and its shape and values must match the
    SFC dataset.
    """
    builder = CustomStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(None, _SFC_REQUEST, _SFC_AXES, ExtractorType.Grib())
    store = builder.build()

    arr = zarr.open_array(store, mode="r")
    assert arr.shape[:2] == (12, 3), f"unexpected root-array shape {arr.shape}"
    # sfc_value(d=0, t=0, p=0) = 0
    assert np.all(arr[0, 0] == 0)
    # sfc_value(d=2, t=3, p=2) = 35
    assert np.all(arr[11, 2] == 35)


def test_root_array_multi_part(read_only_fdb_pattern_setup) -> None:
    """Two add_part(None, ...) calls with extend_on_axis(None, ...) between them.

    Part 1: param=165 → axis-1 index 0
    Part 2: param=166 → axis-1 index 1

    The assembled root array must have shape (12, 2, N).
    """
    axes_one_param = [
        AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
        AxisDefinition(["param"], Chunking.SINGLE_VALUE),
    ]
    builder = CustomStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part(
        None,
        {**_COMMON, "levtype": "sfc",
         "date": ["2020-01-01", "2020-01-02", "2020-01-03"],
         "time": [0, 600, 1200, 1800], "param": [165]},
        axes_one_param,
        ExtractorType.Grib(),
    )
    builder.extend_on_axis(None, 1)
    builder.add_part(
        None,
        {**_COMMON, "levtype": "sfc",
         "date": ["2020-01-01", "2020-01-02", "2020-01-03"],
         "time": [0, 600, 1200, 1800], "param": [166]},
        axes_one_param,
        ExtractorType.Grib(),
    )
    store = builder.build()

    arr = zarr.open_array(store, mode="r")
    assert arr.shape[:2] == (12, 2), f"expected (12, 2, N), got {arr.shape}"
    assert np.all(arr[0, 0] == 0)   # sfc_value(0, 0, 0)
    assert np.all(arr[0, 1] == 1)   # sfc_value(0, 0, 1)


def test_root_array_then_named_raises() -> None:
    """Registering a root array (path=None) then a named path must raise ValueError."""
    builder = CustomStoreBuilder()
    builder.add_part(None, _DUMMY_REQUEST, _DUMMY_AXES, _DUMMY_EXTRACTOR)
    with pytest.raises(ValueError, match="root array"):
        builder.add_part("other", _DUMMY_REQUEST, _DUMMY_AXES, _DUMMY_EXTRACTOR)


def test_named_then_root_raises() -> None:
    """Registering a named path then a root array (path=None) must raise ValueError."""
    builder = CustomStoreBuilder()
    builder.add_part("named", _DUMMY_REQUEST, _DUMMY_AXES, _DUMMY_EXTRACTOR)
    with pytest.raises(ValueError, match="root array"):
        builder.add_part(None, _DUMMY_REQUEST, _DUMMY_AXES, _DUMMY_EXTRACTOR)


def test_top_level_arrays(read_only_fdb_pattern_setup) -> None:
    """Two arrays registered directly at the store root (no parent group).

    Single-segment paths ``"sfc"`` and ``"pl"`` must produce zarr arrays
    accessible directly on the root group — no intervening group level.
    """
    builder = CustomStoreBuilder(read_only_fdb_pattern_setup)
    builder.add_part("sfc", _SFC_REQUEST, _SFC_AXES, ExtractorType.Grib())
    builder.add_part("pl",  _PL_REQUEST,  _PL_AXES,  ExtractorType.Grib())
    store = builder.build()

    root = zarr.open_group(store, mode="r")
    assert "sfc" in root, "top-level array 'sfc' missing from store root"
    assert "pl"  in root, "top-level array 'pl' missing from store root"

    sfc = root["sfc"]
    pl  = root["pl"]

    assert sfc.shape[:2] == (12, 3), f"sfc shape {sfc.shape}"
    assert pl.shape[:2]  == (12, 9), f"pl shape {pl.shape}"

    # sfc_value(d=0, t=0, p=0) = 0
    assert np.all(sfc[0, 0] == 0)
    # pl_value(d=0, t=0, p=0, l=0) = 36
    assert np.all(pl[0, 0] == 36)
