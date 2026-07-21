import pytest
import numpy as np
from z3fdb.custom_store_builder import CustomStoreBuilder, VGroup, VArray
import zarr

from z3fdb import (
    AxisDefinition,
    Chunking,
    SimpleStoreBuilder,
    ExtractorType,
)


# ---------------------------------------------------------------------------
# Unit tests for CustomStoreBuilder._merge_vgroup
# ---------------------------------------------------------------------------
# These tests exercise the group-hierarchy merging logic in isolation,
# without requiring an FDB setup.  They document the expected contract and
# will expose implementation bugs that must be fixed before the function is
# production-ready.
# ---------------------------------------------------------------------------


def _make_builder() -> CustomStoreBuilder:
    """Return a fresh builder with no FDB config (structure-only usage)."""
    return CustomStoreBuilder(fdb_config_file=None)


class TestMergeGroupRaisesOnRootGroup:
    """_merge_vgroup must reject a VGroup that has no parents (would be a second root)."""

    def test_raises_runtime_error(self):
        builder = _make_builder()
        orphan = VGroup(parents=None, name="orphan", children=[])
        with pytest.raises(RuntimeError):
            builder._merge_vgroup(orphan)


class TestMergeGroupDirectChildOfRoot:
    """A group whose sole parent is the tree root must be appended to root.children."""

    def test_group_appears_in_root_children(self):
        builder = _make_builder()
        group_a = VGroup(parents=[builder.root], name="group_a", children=[])
        builder._merge_vgroup(group_a)
        assert group_a in builder.root.children

    def test_root_has_exactly_one_child_after_single_merge(self):
        builder = _make_builder()
        group_a = VGroup(parents=[builder.root], name="group_a", children=[])
        builder._merge_vgroup(group_a)
        assert len(builder.root.children) == 1


class TestMergeGroupSiblings:
    """Multiple groups sharing the same parent must all appear as siblings."""

    def test_two_siblings_at_root_level(self):
        builder = _make_builder()
        root = builder.root
        group_a = VGroup(parents=[root], name="group_a", children=[])
        group_b = VGroup(parents=[root], name="group_b", children=[])
        builder._merge_vgroup(group_a)
        builder._merge_vgroup(group_b)

        child_names = [c.name for c in builder.root.children]
        assert "group_a" in child_names
        assert "group_b" in child_names

    def test_three_siblings_at_root_level(self):
        builder = _make_builder()
        root = builder.root
        names = ["alpha", "beta", "gamma"]
        groups = [VGroup(parents=[root], name=n, children=[]) for n in names]
        for g in groups:
            builder._merge_vgroup(g)

        child_names = [c.name for c in builder.root.children]
        for n in names:
            assert n in child_names


class TestMergeGroupNesting:
    """Groups must be placed under the correct parent node in the tree."""

    def test_child_placed_under_parent(self):
        """group_b (child of group_a) must end up in group_a.children."""
        builder = _make_builder()
        root = builder.root
        group_a = VGroup(parents=[root], name="group_a", children=[])
        builder._merge_vgroup(group_a)

        group_b = VGroup(parents=[root, group_a], name="group_b", children=[])
        builder._merge_vgroup(group_b)

        assert group_b in group_a.children

    def test_child_not_duplicated_in_root(self):
        """group_b should NOT appear directly under root when it belongs to group_a."""
        builder = _make_builder()
        root = builder.root
        group_a = VGroup(parents=[root], name="group_a", children=[])
        builder._merge_vgroup(group_a)

        group_b = VGroup(parents=[root, group_a], name="group_b", children=[])
        builder._merge_vgroup(group_b)

        root_child_names = [c.name for c in builder.root.children]
        assert "group_b" not in root_child_names
        group_a_child_names = [c.name for c in group_a.children]
        assert "group_b" in group_a_child_names

    def test_deeply_nested_three_levels(self):
        """A three-level hierarchy is assembled correctly layer by layer."""
        builder = _make_builder()
        root = builder.root
        group_a = VGroup(parents=[root], name="group_a", children=[])
        group_b = VGroup(parents=[root, group_a], name="group_b", children=[])
        group_c = VGroup(parents=[root, group_a, group_b], name="group_c", children=[])

        builder._merge_vgroup(group_a)
        builder._merge_vgroup(group_b)
        builder._merge_vgroup(group_c)

        assert group_a in root.children
        assert group_b in group_a.children
        assert group_c in group_b.children


class TestMergeGroupIndependentSubtrees:
    """Groups from separate branches must not interfere with each other."""

    def test_same_name_children_under_different_parents(self):
        """Two groups called 'child' under different parents are distinct nodes."""
        builder = _make_builder()
        root = builder.root
        group_1 = VGroup(parents=[root], name="group_1", children=[])
        group_2 = VGroup(parents=[root], name="group_2", children=[])
        builder._merge_vgroup(group_1)
        builder._merge_vgroup(group_2)

        child_1 = VGroup(parents=[root, group_1], name="child", children=[])
        child_2 = VGroup(parents=[root, group_2], name="child", children=[])
        builder._merge_vgroup(child_1)
        builder._merge_vgroup(child_2)

        assert child_1 in group_1.children
        assert child_2 in group_2.children
        # They are distinct objects even though they share the same name
        assert child_1 is not child_2


class TestMergeGroupDeduplication:
    """Merging the same VGroup object twice must not create duplicate entries."""

    def test_no_duplicate_in_root_children(self):
        builder = _make_builder()
        root = builder.root
        group_a = VGroup(parents=[root], name="group_a", children=[])
        builder._merge_vgroup(group_a)
        builder._merge_vgroup(group_a)

        occurrences = sum(1 for c in builder.root.children if c is group_a)
        assert occurrences == 1

    def test_no_duplicate_in_nested_children(self):
        builder = _make_builder()
        root = builder.root
        group_a = VGroup(parents=[root], name="group_a", children=[])
        builder._merge_vgroup(group_a)

        group_b = VGroup(parents=[root, group_a], name="group_b", children=[])
        builder._merge_vgroup(group_b)
        builder._merge_vgroup(group_b)

        occurrences = sum(1 for c in group_a.children if c is group_b)
        assert occurrences == 1


# ---------------------------------------------------------------------------
# Unit tests for CustomStoreBuilder._build_structure
# ---------------------------------------------------------------------------
# These tests verify that VArray leaves are correctly inserted into the
# VGroup tree and that self.structure tracks them properly.
# ---------------------------------------------------------------------------


class TestBuildStructure:
    """_build_structure must place VArrays in the correct tree location."""

    def test_array_at_root_level(self):
        """A single-element path creates a VArray directly under root."""
        builder = _make_builder()
        builder._build_structure(["my_array"])
        assert len(builder.root.children) == 1
        child = builder.root.children[0]
        assert isinstance(child, VArray)
        assert child.name == "my_array"

    def test_array_under_group(self):
        """A two-element path creates a VGroup with a VArray child."""
        builder = _make_builder()
        builder._build_structure(["group_a", "my_array"])
        assert len(builder.root.children) == 1
        group = builder.root.children[0]
        assert isinstance(group, VGroup)
        assert group.name == "group_a"
        assert len(group.children) == 1
        array = group.children[0]
        assert isinstance(array, VArray)
        assert array.name == "my_array"

    def test_array_under_deeply_nested_groups(self):
        """A deep path creates the full VGroup chain with a VArray at the leaf."""
        builder = _make_builder()
        builder._build_structure(["g1", "g2", "g3", "arr"])
        g1 = builder.root.children[0]
        g2 = g1.children[0]
        g3 = g2.children[0]
        leaf = g3.children[0]
        assert isinstance(leaf, VArray)
        assert leaf.name == "arr"

    def test_same_path_returns_same_builder(self):
        """Calling _build_structure twice with the same path returns the same builder."""
        builder = _make_builder()
        b1 = builder._build_structure(["group", "array"])
        b2 = builder._build_structure(["group", "array"])
        assert b1 is b2

    def test_same_path_does_not_duplicate_varray(self):
        """Calling _build_structure twice with the same path inserts only one VArray."""
        builder = _make_builder()
        builder._build_structure(["group", "array"])
        builder._build_structure(["group", "array"])
        group = builder.root.children[0]
        arrays = [c for c in group.children if isinstance(c, VArray)]
        assert len(arrays) == 1

    def test_structure_maps_path_to_varray(self):
        """self.structure[key] is the VArray node at the given path."""
        builder = _make_builder()
        builder._build_structure(["g", "arr"])
        assert "g/arr" in builder.structure
        varray = builder.structure["g/arr"]
        assert isinstance(varray, VArray)
        assert varray.name == "arr"

    def test_varray_builder_matches_returned_builder(self):
        """The builder stored on the VArray is the same object returned by _build_structure."""
        builder = _make_builder()
        returned = builder._build_structure(["g", "arr"])
        assert builder.structure["g/arr"].builder is returned

    def test_shared_group_prefix_reuses_group_node(self):
        """Two paths sharing a prefix put both arrays under the same VGroup node."""
        builder = _make_builder()
        builder._build_structure(["shared", "array_1"])
        builder._build_structure(["shared", "array_2"])
        assert len(builder.root.children) == 1  # only one "shared" group
        shared = builder.root.children[0]
        array_names = [c.name for c in shared.children if isinstance(c, VArray)]
        assert "array_1" in array_names
        assert "array_2" in array_names

    def test_varray_parent_points_to_containing_group(self):
        """The VArray's parent attribute references its containing VGroup."""
        builder = _make_builder()
        builder._build_structure(["grp", "arr"])
        grp = builder.root.children[0]
        arr = grp.children[0]
        assert isinstance(arr, VArray)
        assert arr.parent is grp


# ---------------------------------------------------------------------------
# Integration-style test (requires live FDB via fixture)
# ---------------------------------------------------------------------------


def test_custom_store_builder_access(read_only_fdb_setup) -> None:
    builder = CustomStoreBuilder(read_only_fdb_setup)

    axis_datetime = AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE)
    axis_param = AxisDefinition(["param"], Chunking.SINGLE_VALUE)

    req_1 = (
        "type=an,class=ea,domain=g,expver=0001,stream=oper,date=2020-01-01/to/2020-01-04"
        ",levtype=sfc,step=0,param=131/132,time=0/to/21/by/3"
    )
    req_2 = (
        "type=an,class=ea,domain=g,expver=0001,stream=oper,date=2020-01-02/to/2020-01-04,"
        "levtype=sfc,step=0,param=167,time=0/to/21/by/3"
    )
    req_3 = (
        "type=an,class=ea,domain=g,expver=0001,stream=oper,date=2020-01-02/to/2020-01-04,"
        "levtype=sfc,step=0,param=131/132,time=0/to/21/by/3"
    )
    req_4 = (
        "type=an,class=ea,domain=g,expver=0001,stream=oper,date=2020-01-01,"
        "levtype=sfc,step=0,param=167,time=0/to/18/by/3"
    )

    builder.add_view(
        ["group_1", "sub_group_1", "array_name_1"],
        req_1,
        [axis_datetime, axis_param],
        ExtractorType.GRIB,
    )
    builder.add_view(
        ["group_1", "sub_group_1", "array_name_2"],
        req_2,
        [axis_datetime, axis_param],
        ExtractorType.GRIB,
    )
    builder.add_view(
        ["group_2", "sub_group_1", "subsub_group_2", "array_name_2"],
        req_3,
        [axis_datetime, axis_param],
        ExtractorType.GRIB,
    )
    builder.add_view(
        ["array_name_3"],
        req_4,
        [axis_datetime, axis_param],
        ExtractorType.GRIB,
    )
    store = builder.build()
    root = zarr.open_group(store, mode="r", zarr_format=3, use_consolidated=False)

    assert "group_1" in root
    assert "group_2" in root
    assert "array_name_3" in root

    assert root["group_1/sub_group_1/array_name_1"]

    print(root["group_1/sub_group_1/array_name_1"].shape)
    print(root["group_1/sub_group_1/array_name_2"].shape)
    print(root["group_2/sub_group_1/subsub_group_2/array_name_2"].shape)
    print(root["array_name_3"].shape)


def test_custom_store_two_arrays_in_different_subgroups_combined_axis(
    read_only_fdb_pattern_setup,
) -> None:
    """Two arrays in separate subgroup branches, each using a combined axis with
    scrambled request ordering — analogous to
    test_random_axis_retrieval_swapped_axis_and_request_combined_axis but using
    CustomStoreBuilder with two independent arrays placed in different parts of
    the zarr hierarchy.

    Array 1  →  group_sfc/sub_sfc/sfc_fields    (levtype=sfc, params 167/165/166)
                combined axis [time, step, param, date], scrambled request order.
    Array 2  →  group_pl/sub_pl/pl_fields       (levtype=pl,  params 132/131/133)
                combined axis [date, param, levelist, time], scrambled request order.
    Array 3  →  group_merged/sub_merged/merged_fields
                built from two add_view calls on the same path (sfc + pl parts)
                merged along axis 1, like test_axis_check_merge.
    """
    builder = CustomStoreBuilder(read_only_fdb_pattern_setup)

    # ── Array 1: surface fields — combined axis, scrambled request order ─────
    req_sfc = (
        "type=an,class=ea,domain=g,expver=0001,stream=oper,"
        "time=18/0/12/6,"
        "date=2020-01-03/2020-01-01/2020-01-02,"
        "levtype=sfc,step=0,"
        "param=167/165/166"
    )
    builder.add_view(
        ["group_sfc", "sub_sfc", "sfc_fields"],
        req_sfc,
        [AxisDefinition(["time", "step", "param", "date"], Chunking.NONE)],
        ExtractorType.GRIB,
    )

    # ── Array 2: pressure-level fields — different combined axis, scrambled order
    req_pl = (
        "type=an,class=ea,domain=g,expver=0001,stream=oper,"
        "time=12/0/6/18,"
        "date=2020-01-02/2020-01-01/2020-01-03,"
        "levtype=pl,step=0,"
        "param=132/131/133,"
        "levelist=100/50/150"
    )
    builder.add_view(
        ["group_pl", "sub_pl", "pl_fields"],
        req_pl,
        [AxisDefinition(["date", "param", "levelist", "time"], Chunking.NONE)],
        ExtractorType.GRIB,
    )

    # ── Array 3: merged sfc + pl fields — two add_view calls, axes merged ────
    # Part A (sfc): 2 dates × 2 times × 2 params → axis 1 size 2
    # Part B (pl):  2 dates × 2 times × 2 params × 2 levels → axis 1 size 4
    # After extend_on_axis(path, 1) the resulting shape is [4, 6]:
    #   axis 0: (01-01,t=0), (01-01,t=600), (01-02,t=0), (01-02,t=600)
    #   axis 1: sfc/165, sfc/166, pl/131@50, pl/131@100, pl/132@50, pl/132@100
    merged_path = ["group_merged", "sub_merged", "merged_fields"]
    builder.add_view(
        merged_path,
        "type=an,class=ea,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01/2020-01-02,time=0/600,levtype=sfc,step=0,param=165/166",
        [
            AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
            AxisDefinition(["param"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )
    builder.add_view(
        merged_path,
        "type=an,class=ea,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01/2020-01-02,time=0/600,levtype=pl,step=0,param=131/132,levelist=50/100",
        [
            AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE),
            AxisDefinition(["param", "levelist"], Chunking.SINGLE_VALUE),
        ],
        ExtractorType.GRIB,
    )
    builder.extend_on_axis(merged_path, 1)

    store = builder.build()
    root = zarr.open_group(store, mode="r", zarr_format=3, use_consolidated=False)

    assert "group_sfc" in root
    assert "group_pl" in root
    assert "group_merged" in root
    sfc_data = root["group_sfc/sub_sfc/sfc_fields"]
    pl_data = root["group_pl/sub_pl/pl_fields"]

    # ── sfc array correctness ────────────────────────────────────────────────
    # Fixture ingests in product(dates, times, params_sfc) order:
    #   dates=[01-01, 01-02, 01-03], times=[0, 600, 1200, 1800], params=[165, 166, 167]
    #   constant field value = date_orig * 12 + time_orig * 3 + param_orig
    #
    # Request scramble (request_idx → original_idx in fixture order):
    #   time:  18→3, 0→0, 12→2, 6→1   ⟹ time_perm  = [3, 0, 2, 1]
    #   date:  03→2, 01→0, 02→1        ⟹ date_perm  = [2, 0, 1]
    #   param: 167→2, 165→0, 166→1     ⟹ param_perm = [2, 0, 1]
    #
    # Combined axis [time, step, param, date] with sizes [4, 1, 3, 3]:
    #   linear_idx = time_req * 9 + param_req * 3 + date_req

    def sfc_expected(time_req, param_req, date_req):
        time_perm  = [3, 0, 2, 1]
        date_perm  = [2, 0, 1]
        param_perm = [2, 0, 1]
        return (
            date_perm[date_req] * 4 * 3
            + time_perm[time_req] * 3
            + param_perm[param_req]
        )

    for t in range(4):
        for p in range(3):
            for d in range(3):
                assert all(sfc_data[t * 9 + p * 3 + d] == sfc_expected(t, p, d))

    # ── pl array correctness ─────────────────────────────────────────────────
    # Fixture ingests in product(dates, times, params_pl, levels) order:
    #   dates=[01-01, 01-02, 01-03], times=[0, 600, 1200, 1800],
    #   params=[131, 132, 133], levels=[50, 100, 150]
    #   constant field value = 36 + date_orig * 36 + time_orig * 9 + param_orig * 3 + level_orig
    #   (36 = offset after the 36 sfc messages)
    #
    # Request scramble (request_idx → original_idx in fixture order):
    #   time:  12→2, 0→0, 6→1, 18→3   ⟹ time_perm  = [2, 0, 1, 3]
    #   date:  02→1, 01→0, 03→2        ⟹ date_perm  = [1, 0, 2]
    #   param: 132→1, 131→0, 133→2     ⟹ param_perm = [1, 0, 2]
    #   level: 100→1, 50→0, 150→2      ⟹ level_perm = [1, 0, 2]
    #
    # Combined axis [date, param, levelist, time] with sizes [3, 3, 3, 4]:
    #   linear_idx = date_req * 36 + param_req * 12 + level_req * 4 + time_req

    def pl_expected(date_req, param_req, level_req, time_req):
        time_perm  = [2, 0, 1, 3]
        date_perm  = [1, 0, 2]
        param_perm = [1, 0, 2]
        level_perm = [1, 0, 2]
        return (
            36
            + date_perm[date_req]   * 4 * 3 * 3
            + time_perm[time_req]   * 3 * 3
            + param_perm[param_req] * 3
            + level_perm[level_req]
        )

    for d in range(3):
        for p in range(3):
            for lv in range(3):
                for t in range(4):
                    assert all(
                        pl_data[d * 36 + p * 12 + lv * 4 + t] == pl_expected(d, p, lv, t)
                    )

    # ── Array 3: merged_fields correctness ───────────────────────────────────
    merged_data = root["group_merged/sub_merged/merged_fields"]

    # Pattern fixture sfc values: date_orig * (4*3) + time_orig * 3 + param_orig
    #   dates=[01-01(0), 01-02(1), 01-03(2)], times=[0(0),600(1),1200(2),1800(3)],
    #   params=[165(0), 166(1), 167(2)]
    # Pattern fixture pl values: 36 + date_orig * (4*3*3) + time_orig * (3*3)
    #                                + param_orig * 3 + level_orig
    #   params=[131(0), 132(1), 133(2)], levels=[50(0), 100(1), 150(2)]

    # axis-0 index 0 → date=01-01 (orig 0), time=0 (orig 0)
    assert np.all(merged_data[0, 0] == 0)    # sfc 165: 0*12 + 0*3 + 0
    assert np.all(merged_data[0, 1] == 1)    # sfc 166: 0*12 + 0*3 + 1
    assert np.all(merged_data[0, 2] == 36)   # pl 131@50:  36 + 0 + 0 + 0*3 + 0
    assert np.all(merged_data[0, 3] == 37)   # pl 131@100: 36 + 0 + 0 + 0*3 + 1
    assert np.all(merged_data[0, 4] == 39)   # pl 132@50:  36 + 0 + 0 + 1*3 + 0
    assert np.all(merged_data[0, 5] == 40)   # pl 132@100: 36 + 0 + 0 + 1*3 + 1

    # axis-0 index 1 → date=01-01 (orig 0), time=600 (orig 1)
    assert np.all(merged_data[1, 0] == 3)    # sfc 165: 0*12 + 1*3 + 0
    assert np.all(merged_data[1, 1] == 4)    # sfc 166: 0*12 + 1*3 + 1
    assert np.all(merged_data[1, 2] == 45)   # pl 131@50:  36 + 0 + 1*9 + 0 + 0
    assert np.all(merged_data[1, 3] == 46)   # pl 131@100: 36 + 0 + 1*9 + 0 + 1
    assert np.all(merged_data[1, 4] == 48)   # pl 132@50:  36 + 0 + 1*9 + 1*3 + 0
    assert np.all(merged_data[1, 5] == 49)   # pl 132@100: 36 + 0 + 1*9 + 1*3 + 1

    # axis-0 index 2 → date=01-02 (orig 1), time=0 (orig 0)
    assert np.all(merged_data[2, 0] == 12)   # sfc 165: 1*12 + 0*3 + 0
    assert np.all(merged_data[2, 1] == 13)   # sfc 166: 1*12 + 0*3 + 1
    assert np.all(merged_data[2, 2] == 72)   # pl 131@50:  36 + 1*36 + 0 + 0 + 0
    assert np.all(merged_data[2, 3] == 73)   # pl 131@100: 36 + 1*36 + 0 + 0 + 1
    assert np.all(merged_data[2, 4] == 75)   # pl 132@50:  36 + 1*36 + 0 + 1*3 + 0
    assert np.all(merged_data[2, 5] == 76)   # pl 132@100: 36 + 1*36 + 0 + 1*3 + 1
