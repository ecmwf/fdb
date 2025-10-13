# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

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

from typing import AsyncIterator, Iterable

import numpy as np
import itertools

from zarr.abc import store
from zarr.core.buffer import default_buffer_prototype
from zarr.core.buffer.core import Buffer as AbstractBuffer
from zarr.core.buffer.core import BufferPrototype
from zarr.core.buffer.cpu import Buffer as CpuBuffer
from zarr.core.common import BytesLike

from functools import cache
from typing import Self

from pychunked_data_view import (
    ChunkedDataView,
)

from z3fdb.z3fdb_error import Z3fdbError
from dataclasses import KW_ONLY, asdict, dataclass, field
from typing import Any, Optional, Sequence


def to_cpu_buffer(d: dict) -> CpuBuffer:
    return CpuBuffer.from_bytes(json.dumps(d).encode("utf-8"))


def from_cpu_buffer(buf: CpuBuffer) -> dict:
    return json.loads(buf.to_bytes().decode("utf-8"))


@dataclass(frozen=True)
class DotZarrAttributes:
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
    """
    Generates the .zarr metadata for an array.

    If additional fields are introduced, read the documentation about must_understand
    https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#id13

    Most of what happens here has been reverse-engineered from the zarr-python code.
    """

    _: KW_ONLY
    zarr_format: int = 3
    node_type: str = "array"
    shape: tuple[int, ...]
    data_type: str | Sequence[str] | MetadataConfiguration
    chunk_grid: MetadataConfiguration
    chunk_key_encoding: MetadataConfiguration = MetadataConfiguration(
        name="default", configuration={"separator": "/"}
    )
    codecs: Sequence[MetadataConfiguration] = field(
        default_factory=lambda: [
            MetadataConfiguration(name="bytes", configuration={"endian": "little"})
        ]
    )
    fill_value: bool | int | float | None = None
    attributes: Optional[dict[str, str] | DotZarrAttributes] = field(
        default=DotZarrAttributes()
    )
    storage_transformers: Optional[Sequence[MetadataConfiguration]] = None
    dimension_names: Optional[Sequence[str | None]] = None
    # INFO: If additional fields are introduced, read the documentation about must_understand
    # https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#id13


@dataclass()
class DotZarrGroupJson:
    """
    Generates the .zarr metadata for a group.

    If additional fields are introduced, read the documentation about must_understand
    https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#id13

    Most of what happens here has been reverse-engineered from the zarr-python code.
    """

    _: KW_ONLY
    zarr_format: int = 3
    node_type: str = "group"
    attributes: Optional[dict[str, str] | DotZarrAttributes] = DotZarrAttributes()
    # INFO: If additional fields are introduced, read the documentation about must_understand
    # https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#id13


class FdbSource:
    """
    Uses FDB as a backend.
    Data is retrieved from FDB and assembled on each access.
    """

    def __init__(
        self,
        chunked_data_view: ChunkedDataView,
    ) -> None:
        self._chunked_data_view = chunked_data_view

        self._shape = self._chunked_data_view.shape()
        self._chunks = self._chunked_data_view.chunkShape()
        self._chunks_per_dimension = self._chunked_data_view.chunks()

    def create_dot_zarr_json(self) -> CpuBuffer:
        return to_cpu_buffer(
            asdict(
                DotZarrArrayJson(
                    shape=self._shape,
                    chunk_grid=ChunkGridMetadata(chunks=self._chunks),
                    data_type="float32",
                    fill_value=-1.0,
                )
            )
        )

    def __contains__(self, key: tuple[int, ...]) -> bool:
        if len(key) != len(self._shape):
            return False
        if any(
            k < 0 or k >= limit for k, limit in zip(key, self._chunks_per_dimension)
        ):
            return False
        return True

    def chunks(self) -> tuple[int, ...]:
        return self._chunks_per_dimension

    def __getitem__(self, key: tuple[int, ...]) -> CpuBuffer:
        if len(key) != len(self._shape):
            raise KeyError
        if any(
            k < 0 or k >= limit for k, limit in zip(key, self._chunks_per_dimension)
        ):
            raise KeyError
        return CpuBuffer.from_bytes(self._chunked_data_view.at(key))


class FdbZarrArray:
    def __init__(self, *, name: str = "", datasource: FdbSource):
        self._name = name
        self._datasource = datasource
        self._metadata = self._datasource.create_dot_zarr_json()

    def __getitem__(self, key: tuple[str, ...]) -> Buffer | None:
        if key[0] == "zarr.json":
            return self._metadata
        if len(key) > 1:
            assert key[0] == "c"  # Zarr v3 for chunks
            chunk_ids = (int(c) for c in key[1:])
            return self._datasource[*chunk_ids]

    @property
    def name(self) -> str:
        return self._name

    @cache
    def paths(self) -> list[str]:
        """
        Zarr paths associated to this array, this includes .zarray, .zattrs and all chunks.

        Returns
        -------
        list[str]
            A list of paths belonging to this group
        """
        files = ["zarr.json"]
        if len(chunks_per_axis := self._datasource.chunks()) > 0:
            tuples = itertools.product(*[np.arange(0, x) for x in chunks_per_axis])
            chunk_names = ["/".join([str(i) for i in ["c", *t]]) for t in tuples]
            files += chunk_names
        return files

    async def list_prefix(self, prefix: str) -> AsyncIterator[str]:
        new_prefix = prefix.removeprefix("/" + self._name)
        new_prefix_token = new_prefix.split("/")

        if len(new_prefix_token) == 1:
            yield self.name


class FdbZarrGroup:
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
                raise Z3fdbError(
                    "A group with the empty name can only be the root group."
                )
        self._children = {c.name: c for c in children}

    def __getitem__(self, key: tuple[str, ...]) -> Buffer | None:
        if len(key) == 1:
            if key[0] == "zarr.json":
                return self._metadata
        else:
            return self._children[key[0]][*key[1:]]
        raise KeyError(f"Unknown key {key}")

    @property
    def name(self) -> str:
        return self._name

    @property
    def children(self) -> list["FdbZarrArray | FdbZarrGroup"]:
        return list(self._children.values())

    def paths(self) -> list[str]:
        """
        Zarr paths associated to this group, excluding child groups or arrays.

        Returns
        -------
        list[str]
            A list of paths belonging to this group
        """
        return ["zarr.json"]

    async def list_prefix(self, prefix: str) -> AsyncIterator[str]:
        new_prefix = prefix.removeprefix("/" + self._name)
        new_prefix_token = new_prefix.split("/")

        assert len(new_prefix_token) > 0

        if len(new_prefix_token) == 1:
            for k, _ in self._children.items():
                yield k
        else:
            async for i in self._children[new_prefix_token[0]].list_prefix(new_prefix):
                yield i


class FdbZarrStore(store.Store):
    """Provide access to FDB with a MutableMapping.

    .. note:: This is an experimental feature.

    Requires the `pyFDB <https://redis-py.readthedocs.io/>`_
    package to be installed.

    Parameters
    ----------
    """

    def __init__(self, child: FdbZarrGroup | FdbZarrArray):
        super().__init__(read_only=True)

        self._child = child
        self._known_paths = self._build_paths(self._child)
        self._zmetadata = self._consolidate()

    def _build_paths(self, item, parent_path=None) -> list[str]:
        path = f"{parent_path}/{item.name}" if parent_path else item.name
        files = [f"{path}/{f}" if path != "" else f for f in item.paths()]

        if isinstance(item, FdbZarrGroup):
            for child in item.children:
                files += self._build_paths(child, path)

        return files

    def _consolidate(self):
        consolidated_metatdata = {"metadata": {}}

        ENDINGS = ["zarr.json"]

        filtered_paths = (
            path
            for path in self._known_paths
            for key in ENDINGS
            if path is not None and path.endswith(key)
        )

        for path in filtered_paths:
            keys = path.split("/")
            info = self._child[*keys]

            if info is None:
                continue

            consolidated_metatdata["metadata"].update(
                {path: json.loads(info.to_bytes())}
            )

        # Create Buffer
        return CpuBuffer.from_bytes(json.dumps(consolidated_metatdata).encode("utf-8"))

    async def __getitem__(self, key) -> AbstractBuffer | None:
        if key == ".zmetadata":
            return self._zmetadata
        if key == "zarr.json":
            return self._child._metadata

        keys = key.split("/")
        return self._child[*keys]

    def __iter__(self):
        yield from iter(self._known_paths)

    def __len__(self):
        return len(self._known_paths)

    def __setitem__(self, _k, _v):
        raise Z3fdbError("Views into FDB are not writable")

    def __delitem__(self, _k):
        raise Z3fdbError("Views into FDB are not writable")

    def __contains__(self, key) -> bool:
        return key in self._known_paths

    def __eq__(self, value: object) -> bool:
        return self._child == value._child and self._known_paths == value._known_paths

    async def get(
        self,
        key: str,
        prototype: BufferPrototype = default_buffer_prototype(),
        byte_range: store.ByteRequest | None = None,
    ) -> Buffer | None:
        return await self.__getitem__(key)

    def get_partial_values(
        self,
        prototype: BufferPrototype,
        key_ranges: Iterable[tuple[str, store.ByteRequest | None]],
    ) -> list[Buffer | None]:
        pass

    async def exists(self, key: str) -> bool:
        return key in self._known_paths

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
    def supports_partial_writes(self) -> bool:
        return False

    async def set_partial_values(
        self, key_start_values: Iterable[tuple[str, int, BytesLike]]
    ) -> None:
        raise Z3fdbError("Views into FDB are not writable")

    @property
    def supports_listing(self) -> bool:
        return True

    async def list(self) -> AsyncIterator[str]:
        for i in self._known_paths:
            yield i

    def list_prefix(self, prefix: str) -> AsyncIterator[str]:
        return self._child.list_prefix(prefix)

    def list_dir(self, prefix: str) -> AsyncIterator[str]:
        return self._build_paths(self._child, parent_path=prefix)
