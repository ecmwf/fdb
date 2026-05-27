# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

from dataclasses import dataclass
from zarr.abc.store import Store
from pathlib import Path

from z3fdb._internal.zarr import FdbZarrGroup, FdbZarrStore, FdbZarrArray, FdbSource
from pychunked_data_view import (
    ChunkedDataViewBuilder,
    AxisDefinition,
    ExtractorType,
)


@dataclass
class VArray:
    name: str
    parent: "VGroup"
    builder: ChunkedDataViewBuilder


@dataclass
class VGroup:
    parents: list[VGroup] | None
    name: str
    children: list[VGroup | VArray]

    @staticmethod
    def _join_path(group):
        return "/".join(p.name for p in group.parents) if group.parents else ""

    def __eq__(self, value: object, /) -> bool:
        if not isinstance(value, VGroup):
            return False
        self_path = VGroup._join_path(self)
        value_path = VGroup._join_path(value)
        return self_path == value_path and self.name == value.name

    def get_prefix_path(self) -> list[str]:
        if self.parents is None:
            return []
        return [parent.name for parent in self.parents]

    def descent(self, name) -> "VGroup":
        matches = [c for c in self.children if isinstance(c, VGroup) and c.name == name]
        assert len(matches) == 1
        return matches[0]


class CustomStoreBuilder:
    """
    Builds a zarr store backed by FDB.

    Use add_view() to register one or more MARS request views (each producing a
    virtual zarr array) at arbitrary nested paths, then call build() to obtain
    a read-only FdbZarrStore that zarr can open directly.
    """

    def __init__(self, fdb_config_file: Path | None = None):
        self._config = fdb_config_file
        # Maps joined path (e.g. "group_a/sub/array") -> VArray leaf for fast lookup
        self.structure: dict[str, VArray] = {}
        self.root = VGroup(parents=None, name="/", children=[])

    def _merge_vgroup(self, vgroup: VGroup):
        """Insert *vgroup* into the virtual group tree rooted at self.root.

        Navigates the parent chain stored on *vgroup*. At the first level
        where the expected child is not yet present (the divergence point) the
        group is appended and the traversal stops, so the whole subtree
        described by *vgroup* is inserted at the correct position.
        """
        if vgroup.parents is None:
            raise RuntimeError("Not able to have to root groups.")

        current_group = self.root

        # parents[0] is always root; walk subsequent ancestors until we reach
        # the immediate parent of vgroup or hit a divergence.
        for parent in vgroup.parents[1:]:
            child_names = [c.name for c in current_group.children]
            if parent.name not in child_names:
                # Divergence: insert vgroup here and stop
                if vgroup not in current_group.children:
                    current_group.children.append(vgroup)
                return
            current_group = current_group.descent(parent.name)

        if vgroup not in current_group.children:
            current_group.children.append(vgroup)

    def _build_structure(self, path: list[str]) -> ChunkedDataViewBuilder:
        """Return the ChunkedDataViewBuilder for *path*, creating it if needed.

        Also ensures the matching VGroup hierarchy is present in self.root and
        inserts a VArray leaf into its direct parent group.
        """
        key = "/".join(path)
        if key in self.structure:
            return self.structure[key].builder

        group_names = path[:-1]
        array_name = path[-1]

        # Build VGroups level by level so _merge_vgroup always finds its
        # ancestors already present in the tree.
        parents: list[VGroup] = [self.root]
        for group_name in group_names:
            vgroup = VGroup(parents=list(parents), name=group_name, children=[])
            self._merge_vgroup(vgroup)
            # Resolve the canonical (possibly pre-existing) node for this level
            actual = next(
                c
                for c in parents[-1].children
                if isinstance(c, VGroup) and c.name == group_name
            )
            parents.append(actual)

        parent_group = parents[-1]
        builder = ChunkedDataViewBuilder(self._config)
        varray = VArray(name=array_name, parent=parent_group, builder=builder)
        parent_group.children.append(varray)
        self.structure[key] = varray
        return builder

    def add_view(
        self,
        path: list[str],
        mars_request_key_values: str,
        axes: list[AxisDefinition],
        extractor_type: ExtractorType,
    ) -> None:
        """Register a MARS request as a virtual zarr array at *path*.

        Parameters
        ----------
        path:
            Location of the array in the zarr hierarchy, e.g.
            ``["group_a", "sub_group", "my_array"]``.
        mars_request_key_values:
            MARS request string, e.g.
            ``"type=an,class=ea,date=20200101,param=131"``.
        axes:
            Axis definitions describing how the request dimensions map to
            zarr array dimensions.
        extractor_type:
            How to extract data from the FDB response (e.g. GRIB).
        """
        chunked_data_view = self._build_structure(path=path)
        chunked_data_view.add_part(mars_request_key_values, axes, extractor_type)

    def extend_on_axis(self, path: list[str], axis: int) -> None:
        """Extend the view at *path* along the given *axis*.

        Parameters
        ----------
        path:
            Location of the array in the zarr hierarchy (same as used in
            add_view).
        axis:
            Index of the axis to extend.
        """
        chunked_data_view = self._build_structure(path=path)
        chunked_data_view.extend_on_axis(axis)

    def build(self) -> Store:
        """Assemble all registered views into a read-only FdbZarrStore.

        Traverses the VGroup tree rooted at self.root. Each VGroup becomes an
        FdbZarrGroup and each VArray becomes an FdbZarrArray backed by its
        ChunkedDataViewBuilder.
        """

        def _to_fdb_node(node: VGroup | VArray) -> FdbZarrGroup | FdbZarrArray:
            if isinstance(node, VArray):
                return FdbZarrArray(
                    name=node.name,
                    datasource=FdbSource(node.builder.build()),
                )
            children = [_to_fdb_node(c) for c in node.children]
            return FdbZarrGroup(name=node.name, children=children)

        root_children = [_to_fdb_node(c) for c in self.root.children]
        root = FdbZarrGroup(name="", children=root_children)
        return FdbZarrStore(root)
