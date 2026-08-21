# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass, field
from pathlib import Path

from z3fdb._internal.zarr import FdbZarrArray, FdbZarrGroup, FdbZarrStore, FdbSource
from z3fdb.z3fdb_error import Z3fdbError
from pychunked_data_view import (
    ChunkedDataViewBuilder,
    AxisDefinition,
    ExtractorType,
    MarsSelection,
)


@dataclass
class _VArray:
    name: str
    parent: "_VGroup"
    builder: ChunkedDataViewBuilder


@dataclass
class _VGroup:
    parents: "list[_VGroup] | None"
    name: str
    children: "list[_VGroup | _VArray]" = field(default_factory=list)

    @staticmethod
    def _join_path(group: "_VGroup") -> str:
        return "/".join(p.name for p in group.parents) if group.parents else ""

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, _VGroup):
            return False
        return self._join_path(self) == self._join_path(value) and self.name == value.name

    def descent(self, name: str) -> "_VGroup":
        """Return the single child group called *name*.

        Raises:
            ~z3fdb.Z3fdbError: If there is not exactly one. Names are unique by construction, so
                this is an internal invariant, a bare ``assert`` would vanish under
                ``python -O``.
        """
        matches = [c for c in self.children if isinstance(c, _VGroup) and c.name == name]
        if len(matches) != 1:
            raise Z3fdbError(
                f"CustomStoreBuilder: expected exactly one group named {name!r} under "
                f"{self.name!r}, found {len(matches)}."
            )
        return matches[0]


class CustomStoreBuilder:
    """Builds a zarr store backed by FDB with an arbitrary group/array hierarchy.

    Use :meth:`add_part` to register one or more MARS request parts (each
    producing a virtual zarr array) at arbitrary nested paths, then call
    :meth:`build` to obtain a read-only :class:`FdbZarrStore` that zarr can
    open directly.

    Args:
        fdb_config_file: Optional path to an FDB config file. ``None`` (default)
            lets FDB resolve its configuration from the environment.
    """

    _ROOT_KEY = ""  # structure-dict key reserved for the root array (path=None)

    def __init__(self, fdb_config_file: Path | None = None):
        self._config = fdb_config_file
        self._structure: dict[str, _VArray] = {}
        self._root = _VGroup(parents=None, name="/", children=[])

    def _merge_vgroup(self, vgroup: _VGroup) -> None:
        """Insert *vgroup* into the virtual group tree rooted at self._root."""
        if vgroup.parents is None:
            raise RuntimeError("Cannot have two root groups.")

        current_group = self._root
        for parent in vgroup.parents[1:]:
            # _build_structure creates parents top-down, so every ancestor already exists.
            if parent.name not in [c.name for c in current_group.children]:
                raise Z3fdbError(
                    f"CustomStoreBuilder: parent group {parent.name!r} of {vgroup.name!r} does not exist yet."
                )
            current_group = current_group.descent(parent.name)

        if vgroup not in current_group.children:
            current_group.children.append(vgroup)

    @staticmethod
    def _parse_path(path: str) -> list[str]:
        """Convert a zarr-style path string to a list of name segments.

        Leading and trailing slashes are stripped; multiple consecutive slashes
        are collapsed.  An empty result (e.g. ``""`` or ``"/"``) raises
        :exc:`ValueError`.

        Examples::

            "sfc/wind"   -> ["sfc", "wind"]
            "/sfc/wind"  -> ["sfc", "wind"]   # leading slash accepted
            "t2m"        -> ["t2m"]           # top-level array
            ""           -> ValueError
            "/"          -> ValueError
        """
        parts = [p for p in path.split("/") if p]
        if not parts:
            raise ValueError(
                f"CustomStoreBuilder: path must not be empty. "
                f"Got {path!r}. Use a zarr-style path like 'group/array', "
                f"'/group/array', or 'array' for a top-level array."
            )
        return parts

    def _build_structure(self, path: list[str] | None) -> ChunkedDataViewBuilder:
        """Return the ChunkedDataViewBuilder for *path*, creating it if needed.

        Pass ``None`` to obtain the builder for the root array.
        """
        if path is None:
            # Root array - the store root is itself an array, not a group.
            if self._root.children:
                raise ValueError(
                    "CustomStoreBuilder: cannot register a root array (path=None) when "
                    "named paths are already registered. Use a named path instead."
                )
            if self._ROOT_KEY not in self._structure:
                builder = ChunkedDataViewBuilder(self._config)
                self._structure[self._ROOT_KEY] = _VArray(name="", parent=self._root, builder=builder)
            return self._structure[self._ROOT_KEY].builder

        # Named path - cannot mix with a root array.
        if self._ROOT_KEY in self._structure:
            raise ValueError(
                "CustomStoreBuilder: cannot register a named path when a root array "
                "(path=None) is already registered. Use path=None to add more parts "
                "to the root array."
            )

        key = "/".join(path)
        if key in self._structure:
            return self._structure[key].builder

        group_names = path[:-1]
        array_name = path[-1]

        parents: list[_VGroup] = [self._root]
        for group_name in group_names:
            # Collision: a _VArray already occupies this name - cannot reuse as a group.
            if any(isinstance(c, _VArray) and c.name == group_name for c in parents[-1].children):
                raise ValueError(
                    f"CustomStoreBuilder: '{group_name}' is already registered as an array "
                    f"and cannot also be used as a group."
                )
            vgroup = _VGroup(parents=list(parents), name=group_name)
            self._merge_vgroup(vgroup)
            actual = next(c for c in parents[-1].children if isinstance(c, _VGroup) and c.name == group_name)
            parents.append(actual)

        parent_group = parents[-1]
        # Collision: a _VGroup already occupies this name - cannot reuse as an array.
        if any(isinstance(c, _VGroup) and c.name == array_name for c in parent_group.children):
            raise ValueError(
                f"CustomStoreBuilder: '{array_name}' is already registered as a group "
                f"and cannot also be used as an array."
            )
        builder = ChunkedDataViewBuilder(self._config)
        varray = _VArray(name=array_name, parent=parent_group, builder=builder)
        parent_group.children.append(varray)
        self._structure[key] = varray
        return builder

    def add_part(
        self,
        path: str | None,
        mars_request: MarsSelection,
        axes: list[AxisDefinition],
        extractor: ExtractorType.Grib | ExtractorType.GribJump,
    ) -> None:
        """Register a MARS request as a part of a virtual zarr array at *path*.

        Calling this method multiple times with the same *path* adds further
        parts to the same array (equivalent to
        :meth:`ChunkedDataViewBuilder.add_part` called repeatedly).

        Args:
            path: Zarr-style path of the array in the hierarchy,
                e.g. ``"group_a/sub_group/my_array"`` or ``"t2m"`` for a
                top-level (no-group) array.  A leading ``/`` is accepted and
                ignored.  Pass ``None`` to place the array at the store root
                (accessible via ``zarr.open_array(store)``); this is mutually
                exclusive with any named path.
            mars_request: MARS request as a dict mapping keys to values.
            axes: Axis definitions describing how the request dimensions map
                to zarr array dimensions.
            extractor: Extractor configuration (``ExtractorType.Grib`` or
                ``ExtractorType.GribJump``).
        """
        parts = None if path is None else self._parse_path(path)
        builder = self._build_structure(parts)
        builder.add_part(mars_request, axes, extractor)

    def _existing_array(self, path: list[str] | None) -> ChunkedDataViewBuilder:
        """Return the builder for an array already registered at *path*.

        Unlike :meth:`_build_structure` this never creates one. :meth:`extend_on_axis` and
        :meth:`fill_missing_value` *configure* an existing array, so an unknown path is a
        mistake -- usually a typo -- rather than a request for a new empty array. Creating one
        silently would only surface much later, as "must add at least one part" from
        :meth:`build`.

        Args:
            path: Path segments, or ``None`` for the root array.

        Returns:
            ChunkedDataViewBuilder: The builder registered at *path*.

        Raises:
            ValueError: If no array is registered at *path*.
        """
        key = self._ROOT_KEY if path is None else "/".join(path)
        if key not in self._structure:
            known = sorted(k or "<root>" for k in self._structure) or ["none"]
            where = "the root array (path=None)" if path is None else repr("/".join(path))
            raise ValueError(
                f"CustomStoreBuilder: no array registered at {where}. Call add_part first. "
                f"Registered arrays: {', '.join(known)}."
            )
        return self._structure[key].builder

    def extend_on_axis(self, path: str | None, axis: int) -> None:
        """Declare the extension axis of the array at *path*.

        The array must already exist: call :meth:`add_part` for *path* first.

        Args:
            path: Zarr-style path (same format as :meth:`add_part`).
                ``None`` refers to the root array.
            axis: Zero-based index of the axis to extend.

        Raises:
            ValueError: If no array is registered at *path*.
        """
        parts = None if path is None else self._parse_path(path)
        self._existing_array(parts).extend_on_axis(axis)

    def fill_missing_value(self, path: str | None, value: float) -> None:
        """Set the fill value for the array at *path*.

        The array must already exist: call :meth:`add_part` for *path* first.

        Args:
            path: Zarr-style path (same format as :meth:`add_part`). ``None`` refers to the
                root array.
            value: Value written into positions flagged as missing by the GRIB bitmap. Also
                becomes the zarr array's ``fill_value``. Defaults to NaN when not set.

        Raises:
            ValueError: If no array is registered at *path*.
        """
        parts = None if path is None else self._parse_path(path)
        self._existing_array(parts).fill_missing_value(value)

    def build(self) -> FdbZarrStore:
        """Assemble all registered views into a read-only :class:`FdbZarrStore`.

        Returns:
            A zarr-compatible store that can be opened with
            ``zarr.open(store)`` (group hierarchy) or
            ``zarr.open_array(store)`` (when built from a single root array
            registered via ``path=None``).
        """
        # Root-array shortcut: the store root is itself an array.
        if self._ROOT_KEY in self._structure:
            varray = self._structure[self._ROOT_KEY]
            view = varray.builder.build()
            return FdbZarrStore(
                FdbZarrArray(
                    name="",
                    datasource=FdbSource(view, dim_names=varray.builder.dim_names()),
                )
            )

        def _to_fdb_node(node: _VGroup | _VArray) -> FdbZarrGroup | FdbZarrArray:
            if isinstance(node, _VArray):
                view = node.builder.build()
                return FdbZarrArray(
                    name=node.name,
                    datasource=FdbSource(view, dim_names=node.builder.dim_names()),
                )
            children = [_to_fdb_node(c) for c in node.children]
            return FdbZarrGroup(name=node.name, children=children)

        root_children = [_to_fdb_node(c) for c in self._root.children]
        return FdbZarrStore(FdbZarrGroup(name="", children=root_children))
