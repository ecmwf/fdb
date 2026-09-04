# SPDX-FileCopyrightText: 2025 European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""
This module contains all code internal to z3fdb.

This is not part of the supported interface.
"""

import json

try:
    from collections.abc import Buffer
except ImportError:
    # Python < 3.12
    from typing import Union

    Buffer = Union[bytes, bytearray, memoryview]

from typing import AsyncIterator, Iterable, Iterator, Literal

import math
import itertools

from zarr.abc import store
from zarr.core.buffer import default_buffer_prototype
from zarr.core.buffer.core import Buffer as AbstractBuffer
from zarr.core.buffer.core import BufferPrototype
from zarr.core.buffer.cpu import Buffer as CpuBuffer
from zarr.core.common import BytesLike

from typing import Self

from pychunked_data_view import (
    ChunkedDataView,
)

from z3fdb.z3fdb_error import Z3fdbError
from dataclasses import KW_ONLY, asdict, dataclass, field
from typing import Any, Optional, Sequence


def to_cpu_buffer(d: dict) -> CpuBuffer:
    """Serialise *d* to JSON and wrap it in a zarr CPU buffer."""
    return CpuBuffer.from_bytes(json.dumps(d).encode("utf-8"))


def from_cpu_buffer(buf: CpuBuffer) -> dict:
    """Deserialise a zarr CPU buffer to a dict."""
    return json.loads(buf.to_bytes().decode("utf-8"))


@dataclass(frozen=True)
class DotZarrAttributes:
    """Zarr v3 node attributes attached to every group and array node."""

    _: KW_ONLY
    copyright: str = "ecmwf"
    zarr_format: int = 3
    variables: Sequence[dict] = field(default_factory=list)

    def asbytes(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf8")

    def asstring(self) -> str:
        return json.dumps(asdict(self), indent=2)


@dataclass(frozen=True)
class MetadataConfiguration:
    _: KW_ONLY
    name: str
    configuration: dict[str, Any]


@dataclass(frozen=True)
class ChunkGridMetadata(MetadataConfiguration):
    def __init__(self, chunks) -> None:
        super().__init__(name="regular", configuration={"chunk_shape": chunks})


@dataclass
class DotZarrArrayJson:
    """Zarr v3 array metadata written to ``zarr.json`` at each array node.

    If you add fields, check the ``must_understand`` rules in the spec:
    https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#id13
    """

    _: KW_ONLY
    zarr_format: int = 3
    node_type: str = "array"
    shape: tuple[int, ...]
    data_type: str | Sequence[str] | MetadataConfiguration
    chunk_grid: MetadataConfiguration
    chunk_key_encoding: MetadataConfiguration = MetadataConfiguration(name="default", configuration={"separator": "/"})
    codecs: Sequence[MetadataConfiguration] = field(
        default_factory=lambda: [MetadataConfiguration(name="bytes", configuration={"endian": "little"})]
    )
    fill_value: bool | int | float | None = None
    attributes: Optional[dict[str, str] | DotZarrAttributes] = field(default=DotZarrAttributes())
    storage_transformers: Optional[Sequence[MetadataConfiguration]] = None
    dimension_names: Optional[Sequence[str | None]] = None
    # INFO: If additional fields are introduced, read the documentation about must_understand
    # https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#id13


@dataclass()
class DotZarrGroupJson:
    """Zarr v3 group metadata written to ``zarr.json`` at each group node.

    If you add fields, check the ``must_understand`` rules in the spec:
    https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#id13
    """

    _: KW_ONLY
    zarr_format: int = 3
    node_type: str = "group"
    attributes: Optional[dict[str, str] | DotZarrAttributes] = DotZarrAttributes()
    # INFO: If additional fields are introduced, read the documentation about must_understand
    # https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#id13


class FdbSource:
    """Bridge between the C++ ``ChunkedDataView`` and zarr's array protocol.

    Caches shape and chunk-count metadata from the view at construction;
    serves raw chunk buffers on demand via :meth:`__getitem__`.
    """

    def __init__(
        self,
        chunked_data_view: ChunkedDataView,
        dim_names: list[str] | None = None,
    ) -> None:
        self._chunked_data_view = chunked_data_view
        self._dim_names = dim_names

        self._shape = self._chunked_data_view.shape()
        self._chunks = self._chunked_data_view.chunk_shape()
        self._chunks_per_dimension = self._chunked_data_view.chunks()
        self._fill_value = self._chunked_data_view.fill_missing_value()

    def create_dot_zarr_json(self) -> CpuBuffer:
        """Return the ``zarr.json`` metadata buffer for this array."""
        return to_cpu_buffer(
            asdict(
                DotZarrArrayJson(
                    shape=self._shape,
                    chunk_grid=ChunkGridMetadata(chunks=self._chunks),
                    data_type="float32",
                    fill_value=self._fill_value,
                    dimension_names=self._dim_names,
                )
            )
        )

    def contains_chunk(self, coords: tuple[int, ...]) -> bool:
        """Return True if *coords* is a valid chunk index for this array."""
        if len(coords) != len(self._chunks_per_dimension):
            return False
        return all(0 <= c < limit for c, limit in zip(coords, self._chunks_per_dimension))

    def __contains__(self, key: tuple[int, ...]) -> bool:
        return self.contains_chunk(key)

    def chunks(self) -> tuple[int, ...]:
        """Return the per-dimension chunk counts."""
        return self._chunks_per_dimension

    def __getitem__(self, key: tuple[int, ...]) -> CpuBuffer:
        """Fetch and return chunk *key* as a CPU buffer."""
        if len(key) != len(self._shape):
            raise KeyError
        if any(k < 0 or k >= limit for k, limit in zip(key, self._chunks_per_dimension)):
            raise KeyError
        return CpuBuffer.from_bytes(self._chunked_data_view.at(key))


class FdbZarrArray:
    """Zarr v3 array node backed by an :class:`FdbSource`."""

    def __init__(self, *, name: str = "", datasource: FdbSource):
        self._name = name
        self._datasource = datasource
        self._metadata = self._datasource.create_dot_zarr_json()

    def __getitem__(self, key: str) -> AbstractBuffer | None:
        """Route ``zarr.json`` to metadata and ``c/<i>/...`` keys to chunk data."""
        if key == "zarr.json":
            return self._metadata
        if key.startswith("c/"):
            chunk_ids = tuple(int(c) for c in key.split("/")[1:])
            return self._datasource[chunk_ids]
        raise KeyError(key)

    @property
    def name(self) -> str:
        return self._name

    def paths(self) -> list[str]:
        """Return ``['zarr.json']``; chunk keys are not enumerated here."""
        return ["zarr.json"]


class FdbZarrGroup:
    """Zarr v3 group node; delegates key lookups to its named children."""

    def __init__(
        self,
        *,
        name: str = "",
        children: list[Self | FdbZarrArray] = [],
    ):
        self._name = name
        self._metadata = to_cpu_buffer(asdict(DotZarrGroupJson()))
        self._attributes = to_cpu_buffer(asdict(DotZarrAttributes()))
        for c in children:
            if c.name == "":
                raise Z3fdbError("A group with the empty name can only be the root group.")

        # TODO(TKR): Metadata consolidation can happen for groups, wait for the standard to settle, see:
        # https://github.com/zarr-developers/zarr-specs/issues/371
        self._children = {c.name: c for c in children}

    def __getitem__(self, key: str) -> AbstractBuffer | None:
        slash = key.find("/")
        if slash == -1:
            if key == "zarr.json":
                return self._metadata
            raise KeyError(key)
        head, tail = key[:slash], key[slash + 1 :]
        return self._children[head][tail]

    @property
    def name(self) -> str:
        return self._name

    @property
    def children(self) -> list["FdbZarrArray | FdbZarrGroup"]:
        return list(self._children.values())

    def paths(self) -> list[str]:
        """Return ``['zarr.json']`` for this group (children contribute their own paths)."""
        return ["zarr.json"]


class FdbZarrStore(store.Store):
    """Read-only zarr v3 store that virtualises an arbitrary group/array hierarchy.

    ``_known_paths`` holds metadata keys only (one ``zarr.json`` per node).
    Chunk keys are validated O(1) via :meth:`_chunk_key_exists` and generated
    lazily by :meth:`_iter_chunk_paths` - they are never stored.
    """

    def __init__(self, child: FdbZarrGroup | FdbZarrArray):
        super().__init__(read_only=True)
        self._child = child
        # Metadata paths only - chunk keys are generated on demand.
        self._known_paths = self._build_paths(self._child)
        # Flat map: absolute array path -> FdbZarrArray, for chunk operations.
        self._arrays: dict[str, FdbZarrArray] = self._collect_arrays(self._child)
        self._root_zarr_json = self._build_root_zarr_json()

    def _collect_arrays(
        self,
        item: "FdbZarrGroup | FdbZarrArray",
        parent_path: str = "",
    ) -> "dict[str, FdbZarrArray]":
        """Return a flat dict mapping each array's absolute path to its node."""
        path = f"{parent_path}/{item.name}" if parent_path else item.name
        if isinstance(item, FdbZarrArray):
            return {path: item}
        result: dict[str, FdbZarrArray] = {}
        for c in item.children:
            result.update(self._collect_arrays(c, path))
        return result

    def _chunk_key_exists(self, key: str) -> bool:
        """Return True if *key* is a valid chunk key for any array, without enumeration."""
        for arr_path, arr in self._arrays.items():
            chunk_prefix = (arr_path + "/c/") if arr_path else "c/"
            if key.startswith(chunk_prefix):
                try:
                    coords = tuple(int(c) for c in key[len(chunk_prefix) :].split("/"))
                except ValueError:
                    return False
                return arr._datasource.contains_chunk(coords)
        return False

    def _iter_chunk_paths(self) -> Iterator[str]:
        """Yield every chunk key for every array, built at call time."""
        for arr_path, arr in self._arrays.items():
            prefix = (arr_path + "/c/") if arr_path else "c/"
            for coords in itertools.product(*[range(n) for n in arr._datasource.chunks()]):
                yield prefix + "/".join(str(c) for c in coords)

    def _build_paths(self, item, parent_path=None) -> list[str]:
        """Collect metadata paths for *item* and all its descendants."""
        path = f"{parent_path}/{item.name}" if parent_path else item.name
        files = [f"{path}/{f}" if path != "" else f for f in item.paths()]

        if isinstance(item, FdbZarrGroup):
            for child in item.children:
                files += self._build_paths(child, path)

        return files

    def _build_root_zarr_json(self) -> CpuBuffer:
        """Build the root ``zarr.json`` with inline ``consolidated_metadata`` for the full hierarchy."""
        # Only root groups can carry consolidated metadata in zarr v3.
        # Root arrays are terminal nodes; no consolidated_metadata concept applies.
        if not isinstance(self._child, FdbZarrGroup):
            return self._child._metadata

        # Collect raw metadata for every node, keyed by absolute node path.
        # "" is the root group; all others are non-root descendants.
        raw: dict[str, dict] = {}
        for path in self._known_paths:
            if not path.endswith("zarr.json") or path == "zarr.json":
                continue
            node_path = path.removesuffix("/zarr.json")
            info = self._child[path]
            if info is None:
                continue
            raw[node_path] = json.loads(info.to_bytes())

        def _descendants(group_abs_path: str) -> dict[str, dict]:
            """Return all descendants of a group keyed by path relative to that group.

            Values are the raw node metadata dicts (no consolidated_metadata added),
            sorted shallower-first then alphabetically.
            """
            prefix = (group_abs_path + "/") if group_abs_path else ""
            result: dict[str, dict] = {}
            for abs_path, meta in raw.items():
                if not abs_path or abs_path == group_abs_path:
                    continue
                if prefix and not abs_path.startswith(prefix):
                    continue
                result[abs_path[len(prefix) :]] = meta
            return dict(sorted(result.items(), key=lambda kv: (kv[0].count("/"), kv[0])))

        # Build the flat metadata dict.  Every group gets a consolidated_metadata
        # whose metadata contains all its descendants with relative paths and plain
        # (no consolidated_metadata) values - groups within are raw group JSON only.
        flat: dict[str, dict] = {}
        for abs_path, meta in raw.items():
            if not abs_path:
                continue  # root handled separately below
            node_meta = dict(meta)
            if meta.get("node_type") == "group":
                node_meta["consolidated_metadata"] = {
                    "kind": "inline",
                    "must_understand": False,
                    "metadata": _descendants(abs_path),
                }
            flat[abs_path] = node_meta

        # Sort: shallower nodes first, then alphabetically (mirrors zarr-python output)
        sorted_flat = dict(sorted(flat.items(), key=lambda kv: (kv[0].count("/"), kv[0])))

        root_meta = json.loads(self._child._metadata.to_bytes())
        root_meta["consolidated_metadata"] = {
            "kind": "inline",
            "must_understand": False,
            "metadata": sorted_flat,
        }
        return CpuBuffer.from_bytes(json.dumps(root_meta).encode("utf-8"))

    async def __getitem__(self, key) -> AbstractBuffer | None:
        if key == "zarr.json":
            return self._root_zarr_json  # includes consolidated_metadata for group roots
        return self._child[key]

    def __iter__(self):
        yield from self._known_paths
        yield from self._iter_chunk_paths()

    def __len__(self):
        chunk_total = sum(math.prod(arr._datasource.chunks()) for arr in self._arrays.values())
        return len(self._known_paths) + chunk_total

    def __setitem__(self, _k, _v):
        raise Z3fdbError("Views into FDB are not writable")

    def __delitem__(self, _k):
        raise Z3fdbError("Views into FDB are not writable")

    def __contains__(self, key) -> bool:
        return key in self._known_paths or self._chunk_key_exists(key)

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, FdbZarrStore):
            return False
        return self._child == value._child and self._known_paths == value._known_paths

    async def get(
        self,
        key: str,
        prototype: BufferPrototype = default_buffer_prototype(),
        byte_range: store.ByteRequest | None = None,
    ) -> AbstractBuffer | None:
        if byte_range is not None:
            raise Z3fdbError("Partial values aren't supported yet.")
        try:
            return await self.__getitem__(key)
        except KeyError:
            return None

    async def get_partial_values(
        self,
        prototype: BufferPrototype,
        key_ranges: Iterable[tuple[str, store.ByteRequest | None]],
    ) -> list[AbstractBuffer | None]:
        raise Z3fdbError("Partial values aren't supported yet.")

    async def exists(self, key: str) -> bool:
        return key in self

    @property
    def supports_writes(self) -> bool:
        return False

    async def set(self, key: str, value: AbstractBuffer) -> None:
        raise Z3fdbError("Views into FDB are not writable")

    async def set_if_not_exists(self, key: str, value: AbstractBuffer) -> None:
        raise Z3fdbError("Views into FDB are not writable")

    async def _set_many(self, values: Iterable[tuple[str, AbstractBuffer]]) -> None:
        return await super()._set_many(values)

    @property
    def supports_deletes(self) -> bool:
        return False

    async def delete(self, key: str) -> None:
        raise Z3fdbError("Views into FDB are not writable")

    @property
    def supports_partial_writes(self) -> Literal[False]:
        return False

    async def set_partial_values(self, key_start_values: Iterable[tuple[str, int, BytesLike]]) -> None:
        raise Z3fdbError("Views into FDB are not writable")

    @property
    def supports_listing(self) -> bool:
        return True

    async def list(self) -> AsyncIterator[str]:
        for path in self._known_paths:
            yield path
        for path in self._iter_chunk_paths():
            yield path

    async def list_prefix(self, prefix: str) -> AsyncIterator[str]:
        for path in self._known_paths:
            if path.startswith(prefix):
                yield path
        for path in self._iter_chunk_paths():
            if path.startswith(prefix):
                yield path

    async def list_dir(self, prefix: str) -> AsyncIterator[str]:
        # Normalize so the scan prefix ends with "/"
        scan_prefix = (prefix.rstrip("/") + "/") if prefix else ""
        seen: set[str] = set()
        for path in self._known_paths:
            if path.startswith(scan_prefix):
                child = path[len(scan_prefix) :].split("/")[0]
                if child and child not in seen:
                    seen.add(child)
                    yield child
        for path in self._iter_chunk_paths():
            if path.startswith(scan_prefix):
                child = path[len(scan_prefix) :].split("/")[0]
                if child and child not in seen:
                    seen.add(child)
                    yield child
