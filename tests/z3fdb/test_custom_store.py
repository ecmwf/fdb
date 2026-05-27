import pytest
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
